//! Lossy float compression codec benchmark harness (test-only, not production).
//!
//! Compares brotli / lz4_raw / zstd / gzip on compression ratio and CPU cost
//! for floating-point columns going through the real QuestDB parquet write
//! pipeline, and shows how BYTE_STREAM_SPLIT encoding and lossy mantissa
//! rounding interact with each codec.
//!
//! This module lives inside the lib crate because `lossy::round_f64` is
//! `pub(crate)` and `ParquetEncodingConfig::new` is `cfg(test)` only.
//!
//! Run (RELEASE is mandatory for meaningful timings):
//!   cargo test --release -p qdbr --lib bench_codecs::run_codec_bench \
//!       -- --ignored --nocapture

use std::io::Cursor;
use std::ptr::null;
use std::time::Instant;

use arrow::array::{Float32Array, Float64Array, Int64Array};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pco::data_types::Number;
use qdb_core::col_type::ColumnTypeTag;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::parquet::tests::ColumnTypeTagExt;
use crate::parquet_write::file::ParquetWriter;
use crate::parquet_write::lossy::{round_f32, round_f64};
use crate::parquet_write::schema::{Column, ParquetEncodingConfig, Partition};

const N: usize = 1_000_000;
const RUNS: usize = 3;

/// Compression codec ids as packed by `ParquetEncodingConfig::new`
/// (see schema.rs `compression()` match arms).
#[derive(Clone, Copy)]
struct Codec {
    name: &'static str,
    id: i32,
    /// Semantic level passed to `ParquetEncodingConfig::new` (-1 = codec default).
    level: i32,
    /// Human-readable level note for the report.
    level_note: &'static str,
}

const CODECS: &[Codec] = &[
    Codec {
        name: "uncompressed",
        id: 1,
        level: -1,
        level_note: "n/a",
    },
    Codec {
        name: "lz4_raw",
        id: 6,
        level: -1,
        level_note: "n/a (lz4_raw has no level)",
    },
    Codec {
        name: "zstd",
        id: 5,
        level: -1,
        level_note: "level 1 (schema.rs default sentinel -> 1)",
    },
    Codec {
        name: "gzip",
        id: 3,
        level: -1,
        level_note: "level 6 (schema.rs default sentinel -> 6)",
    },
    Codec {
        name: "brotli",
        id: 4,
        level: -1,
        level_note: "level 1 (schema.rs default sentinel -> 1)",
    },
];

/// (label, encoding id, keep mantissa bits). keep>=52 means no rounding.
struct RowSpec {
    keep: u32,
    encoding_id: i32,
    enc_label: &'static str,
}

fn box_muller(rng: &mut StdRng) -> f64 {
    // Draw a standard normal via Box-Muller. u1 in (0,1] to avoid ln(0).
    let u1: f64 = 1.0 - rng.random::<f64>();
    let u2: f64 = rng.random::<f64>();
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}

/// Price-like: geometric random walk, smooth and highly autocorrelated.
fn gen_price_like(seed: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut out = Vec::with_capacity(N);
    let mut p = 100.0_f64;
    for _ in 0..N {
        p *= (0.0002 * box_muller(&mut rng)).exp();
        out.push(p);
    }
    out
}

/// Qty-like: noisy positive values, heavy-tailed, low autocorrelation.
/// abs(N(0,1)) * 10^U(-2,4).
fn gen_qty_like(seed: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut out = Vec::with_capacity(N);
    for _ in 0..N {
        let mag = box_muller(&mut rng).abs();
        let exp: f64 = -2.0 + rng.random::<f64>() * 6.0; // U(-2,4)
        out.push(mag * 10f64.powf(exp));
    }
    out
}

fn apply_rounding(src: &[f64], keep: u32) -> Vec<f64> {
    src.iter().map(|&x| round_f64(x, keep)).collect()
}

/// Realized (max, mean) relative error of `rounded` vs `orig`, skipping zeros.
fn rel_error(orig: &[f64], rounded: &[f64]) -> (f64, f64) {
    let mut max = 0.0_f64;
    let mut sum = 0.0_f64;
    let mut cnt = 0usize;
    for (&o, &r) in orig.iter().zip(rounded.iter()) {
        if o == 0.0 {
            continue;
        }
        let e = ((r - o) / o).abs();
        if e > max {
            max = e;
        }
        sum += e;
        cnt += 1;
    }
    let mean = if cnt > 0 { sum / cnt as f64 } else { 0.0 };
    (max, mean)
}

/// Encode `data` to an in-memory parquet file with the given encoding+codec.
/// Returns (file bytes, encode time ms).
fn encode_to_parquet(data: &[f64], encoding_id: i32, codec: &Codec) -> (Vec<u8>, f64) {
    let config = ParquetEncodingConfig::new(encoding_id, codec.id, codec.level).raw();
    let start = Instant::now();
    let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
    let col = Column::from_raw_data(
        0,
        "val",
        ColumnTypeTag::Double.into_type().code(),
        0,
        data.len(),
        data.as_ptr() as *const u8,
        std::mem::size_of_val(data),
        null(),
        0,
        null(),
        0,
        false,
        false,
        config,
    )
    .expect("column");
    let partition = Partition { table: "bench".to_string(), columns: vec![col] };
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .finish(partition)
        .expect("write parquet");
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    (buf.into_inner(), elapsed)
}

/// Decode the single Float64 column from a parquet file via arrow. Returns
/// (decoded values, decode time ms). Sums values to defeat dead-code elision.
fn decode_from_parquet(bytes: &[u8]) -> (usize, f64) {
    let bytes: Bytes = Bytes::copy_from_slice(bytes);
    let start = Instant::now();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .expect("reader")
        .with_batch_size(65536)
        .build()
        .expect("build reader");
    let mut count = 0usize;
    let mut acc = 0.0_f64;
    for batch in reader.flatten() {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64 array");
        count += arr.len();
        // Touch a few values so the decode is not optimized away.
        if !arr.is_empty() {
            acc += arr.value(0) + arr.value(arr.len() - 1);
        }
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    std::hint::black_box(acc);
    (count, elapsed)
}

fn implied_max_rel_err(keep: u32) -> f64 {
    2f64.powi(-(keep as i32 + 1))
}

fn run_dataset(title: &str, data: &[f64]) {
    // PLAIN baselines (full precision) to show the encoding's effect, then the
    // full BYTE_STREAM_SPLIT matrix across rounding levels.
    let mut specs: Vec<RowSpec> = vec![RowSpec { keep: 53, encoding_id: 1, enc_label: "PLAIN" }];
    for keep in [53u32, 23, 16, 12, 10] {
        specs.push(RowSpec { keep, encoding_id: 5, enc_label: "BSS" });
    }

    println!("\n## {title}");
    println!("\n| encoding | keep_bits | ~max rel err | codec | bytes/value | ratio vs 8.0 | encode ms | decode ms | realized max rel err | realized mean rel err |");
    println!("|---|---|---|---|---|---|---|---|---|---|");

    for spec in &specs {
        let rounded = apply_rounding(data, spec.keep);
        let (rmax, rmean) = rel_error(data, &rounded);
        let keep_disp = if spec.keep >= 52 {
            "full".to_string()
        } else {
            spec.keep.to_string()
        };
        let implied = if spec.keep >= 52 {
            "0".to_string()
        } else {
            format!("{:.2e}", implied_max_rel_err(spec.keep))
        };

        for codec in CODECS {
            // Average encode/decode over RUNS; size is deterministic.
            let mut enc_ms = 0.0;
            let mut dec_ms = 0.0;
            let mut file_len = 0usize;
            for run in 0..RUNS {
                let (bytes, e) = encode_to_parquet(&rounded, spec.encoding_id, codec);
                let (cnt, d) = decode_from_parquet(&bytes);
                assert_eq!(cnt, rounded.len());
                enc_ms += e;
                dec_ms += d;
                if run == 0 {
                    file_len = bytes.len();
                }
            }
            enc_ms /= RUNS as f64;
            dec_ms /= RUNS as f64;
            let bytes_per_val = file_len as f64 / rounded.len() as f64;
            let ratio = 8.0 / bytes_per_val;
            println!(
                "| {} | {} | {} | {} | {:.3} | {:.2}x | {:.1} | {:.1} | {:.2e} | {:.2e} |",
                spec.enc_label,
                keep_disp,
                implied,
                codec.name,
                bytes_per_val,
                ratio,
                enc_ms,
                dec_ms,
                rmax,
                rmean,
            );
        }

        // pco row (round -> pco). pco is a fitted numeric codec, not a Parquet
        // encoding, so this is raw codec bytes (no Parquet framing); the framing
        // overhead in the rows above is a few KB, negligible at N=1M.
        let (pco_bytes, pco_enc, pco_dec) = bench_pco(&rounded);
        let bpv = pco_bytes as f64 / rounded.len() as f64;
        println!(
            "| pco | {} | {} | pco-8 | {:.3} | {:.2}x | {:.1} | {:.1} | {:.2e} | {:.2e} |",
            keep_disp,
            implied,
            bpv,
            8.0 / bpv,
            pco_enc,
            pco_dec,
            rmax,
            rmean,
        );
    }
}

/// Compress with pco (level 8) and round-trip-decompress for timing+correctness.
/// Returns (compressed bytes, mean encode ms, mean decode ms) over RUNS.
fn bench_pco<T: Number>(data: &[T]) -> (usize, f64, f64) {
    let cfg = pco::ChunkConfig::default().with_compression_level(8);
    let mut enc_ms = 0.0;
    let mut dec_ms = 0.0;
    let mut len = 0usize;
    for run in 0..RUNS {
        let t0 = Instant::now();
        let bytes = pco::standalone::simple_compress(data, &cfg).expect("pco compress");
        enc_ms += t0.elapsed().as_secs_f64() * 1000.0;
        let t1 = Instant::now();
        let decoded: Vec<T> = pco::standalone::simple_decompress(&bytes).expect("pco decompress");
        dec_ms += t1.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(decoded.len(), data.len());
        std::hint::black_box(&decoded);
        if run == 0 {
            len = bytes.len();
        }
    }
    (len, enc_ms / RUNS as f64, dec_ms / RUNS as f64)
}

#[test]
#[ignore = "benchmark; run in release with --ignored --nocapture"]
fn run_codec_bench() {
    println!("\n# Lossy float compression codec benchmark");
    println!("N = {N} values per dataset, {RUNS} runs averaged for timings.");
    println!("Codec levels:");
    for c in CODECS {
        println!("  - {}: {}", c.name, c.level_note);
    }

    let price = gen_price_like(0xC0FFEE);
    let qty = gen_qty_like(0xBADF00D);

    run_dataset("price-like (geometric random walk, smooth)", &price);
    run_dataset("qty-like (heavy-tailed abs-normal * 10^U(-2,4))", &qty);
}

// ---- Real-data confirmation: round -> BSS+zstd (shipped path) vs round -> pco ----
// f32 columns, matching the source dtype and the earlier numpy proxy benchmark.

const REAL_MAX_ROWS: usize = 50_000_000; // middle contiguous slice cap (matches proxy)

/// Encode an f32 slice to an in-memory parquet `Float` column. Returns (bytes, ms).
fn encode_to_parquet_f32(data: &[f32], encoding_id: i32, codec: &Codec) -> (Vec<u8>, f64) {
    let config = ParquetEncodingConfig::new(encoding_id, codec.id, codec.level).raw();
    let start = Instant::now();
    let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
    let col = Column::from_raw_data(
        0,
        "val",
        ColumnTypeTag::Float.into_type().code(),
        0,
        data.len(),
        data.as_ptr() as *const u8,
        std::mem::size_of_val(data),
        null(),
        0,
        null(),
        0,
        false,
        false,
        config,
    )
    .expect("column");
    let partition = Partition { table: "bench".to_string(), columns: vec![col] };
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .finish(partition)
        .expect("write parquet");
    (buf.into_inner(), start.elapsed().as_secs_f64() * 1000.0)
}

fn decode_from_parquet_f32(bytes: &[u8]) -> (usize, f64) {
    let bytes: Bytes = Bytes::copy_from_slice(bytes);
    let start = Instant::now();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .expect("reader")
        .with_batch_size(65536)
        .build()
        .expect("build reader");
    let mut count = 0usize;
    let mut acc = 0.0_f32;
    for batch in reader.flatten() {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("f32 array");
        count += arr.len();
        if !arr.is_empty() {
            acc += arr.value(0) + arr.value(arr.len() - 1);
        }
    }
    std::hint::black_box(acc);
    (count, start.elapsed().as_secs_f64() * 1000.0)
}

/// Minimal .npy loader for 1-D `<f4`/`<f8` arrays. Reads the whole file, then
/// keeps a middle contiguous slice of at most REAL_MAX_ROWS finite values.
fn load_npy_f32(path: &str) -> Option<Vec<f32>> {
    let raw = std::fs::read(path).ok()?;
    if raw.len() < 12 || &raw[0..6] != b"\x93NUMPY" {
        return None;
    }
    let major = raw[6];
    let (hdr_len, data_start) = if major == 1 {
        let l = u16::from_le_bytes([raw[8], raw[9]]) as usize;
        (l, 10 + l)
    } else {
        let l = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]) as usize;
        (l, 12 + l)
    };
    let header = std::str::from_utf8(&raw[data_start - hdr_len..data_start]).ok()?;
    let data = &raw[data_start..];
    let (elem, is_f8) = if header.contains("<f4") {
        (4usize, false)
    } else if header.contains("<f8") {
        (8usize, true)
    } else {
        return None;
    };
    let n_total = data.len() / elem;
    let (start_el, count) = if n_total > REAL_MAX_ROWS {
        ((n_total - REAL_MAX_ROWS) / 2, REAL_MAX_ROWS)
    } else {
        (0, n_total)
    };
    let region = &data[start_el * elem..(start_el + count) * elem];
    let mut out = Vec::with_capacity(count);
    for c in region.chunks_exact(elem) {
        let v = if is_f8 {
            f64::from_le_bytes(c.try_into().unwrap()) as f32
        } else {
            f32::from_le_bytes([c[0], c[1], c[2], c[3]])
        };
        if v.is_finite() {
            out.push(v);
        }
    }
    Some(out)
}

fn run_real_dataset(name: &str, data: &[f32]) {
    let bss = CODECS.iter().find(|c| c.name == "zstd").unwrap();
    println!("\n## {name}  (n={}, f32)", data.len());
    println!("\n| keep_bits | pipeline | bytes/value | ratio vs 4.0 | encode ms | decode ms | realized max rel err |");
    println!("|---|---|---|---|---|---|---|");
    for keep in [23u32, 12, 11] {
        let rounded: Vec<f32> = data.iter().map(|&x| round_f32(x, keep)).collect();
        let (rmax, _) = rel_error_f32(data, &rounded);
        let keep_disp = if keep >= 23 {
            "full".to_string()
        } else {
            keep.to_string()
        };

        // round -> BYTE_STREAM_SPLIT -> zstd (the shipped lossy path).
        let mut enc = 0.0;
        let mut dec = 0.0;
        let mut len = 0usize;
        for run in 0..RUNS {
            let (b, e) = encode_to_parquet_f32(&rounded, 5, bss);
            let (cnt, d) = decode_from_parquet_f32(&b);
            assert_eq!(cnt, rounded.len());
            enc += e;
            dec += d;
            if run == 0 {
                len = b.len();
            }
        }
        let bpv = len as f64 / rounded.len() as f64;
        println!(
            "| {} | BSS+zstd | {:.3} | {:.2}x | {:.1} | {:.1} | {:.2e} |",
            keep_disp,
            bpv,
            4.0 / bpv,
            enc / RUNS as f64,
            dec / RUNS as f64,
            rmax,
        );

        // round -> pco.
        let (pb, pe, pd) = bench_pco(&rounded);
        let pbpv = pb as f64 / rounded.len() as f64;
        println!(
            "| {} | pco-8 | {:.3} | {:.2}x | {:.1} | {:.1} | {:.2e} |",
            keep_disp,
            pbpv,
            4.0 / pbpv,
            pe,
            pd,
            rmax,
        );
    }
}

fn rel_error_f32(orig: &[f32], rounded: &[f32]) -> (f64, f64) {
    let mut max = 0.0_f64;
    let mut sum = 0.0_f64;
    let mut cnt = 0usize;
    for (&o, &r) in orig.iter().zip(rounded.iter()) {
        if o == 0.0 {
            continue;
        }
        let e = (((r - o) as f64) / o as f64).abs();
        if e > max {
            max = e;
        }
        sum += e;
        cnt += 1;
    }
    (max, if cnt > 0 { sum / cnt as f64 } else { 0.0 })
}

#[test]
#[ignore = "benchmark; needs /tmp/real_*.npy; run in release with --ignored --nocapture"]
fn run_pco_real_bench() {
    println!("\n# Real-data: round -> BSS+zstd (shipped) vs round -> pco");
    let mut files: Vec<String> = std::fs::read_dir("/tmp")
        .expect("read /tmp")
        .flatten()
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| n.starts_with("real_") && n.ends_with(".npy"))
        .map(|n| format!("/tmp/{n}"))
        .collect();
    files.sort();
    if files.is_empty() {
        println!("(no /tmp/real_*.npy files found; skipping)");
        return;
    }
    for f in files {
        match load_npy_f32(&f) {
            Some(data) if !data.is_empty() => run_real_dataset(&f, &data),
            _ => println!("\n## {f}  (could not load; skipped)"),
        }
    }
}

/// pco bytes/value for the SAME logical values stored as f32 vs f64.
/// Regime 1: f32-origin values (exactly representable in both widths), so the
///   f32 and f64 columns carry identical information -- isolates container width.
/// Regime 2: genuine 52-bit f64 random walks; the f32 column is a lossy cast,
///   reported with its realized max rel err so the comparison stays honest.
#[test]
#[ignore = "benchmark; run in release with --ignored --nocapture"]
fn run_pco_f32_vs_f64_bench() {
    println!("\n# pco density: same values as f32 vs f64");
    println!("bytes/value via pco-8 (lossless w.r.t. the stored value). RUNS={RUNS}.");

    // Regime 1: identical f32-precision values in both containers.
    if let Some(f32_data) = load_npy_f32("/tmp/arcticbench/sample.npy") {
        println!(
            "\n## Regime 1 -- f32-origin prices (n={}, identical info in both widths)",
            f32_data.len()
        );
        println!("\n| keep_bits | f32 B/val | f64 B/val | f64/f32 |");
        println!("|---|---|---|---|");
        for keep in [23u32, 16, 12, 11] {
            let r32: Vec<f32> = f32_data.iter().map(|&x| round_f32(x, keep)).collect();
            let r64: Vec<f64> = r32.iter().map(|&x| x as f64).collect();
            let (b32, _, _) = bench_pco(&r32);
            let (b64, _, _) = bench_pco(&r64);
            let bpv32 = b32 as f64 / r32.len() as f64;
            let bpv64 = b64 as f64 / r64.len() as f64;
            let keep_disp = if keep >= 23 {
                "full".to_string()
            } else {
                keep.to_string()
            };
            println!(
                "| {} | {:.3} | {:.3} | {:.2}x |",
                keep_disp,
                bpv32,
                bpv64,
                bpv64 / bpv32,
            );
        }
    } else {
        println!("\n## Regime 1 -- skipped (no /tmp/arcticbench/sample.npy)");
    }

    // Regime 2: genuine 52-bit f64; the f32 path is a lossy cast.
    for (name, data) in [
        (
            "price-like (geometric random walk)",
            gen_price_like(0xC0FFEE),
        ),
        ("qty-like (heavy-tailed)", gen_qty_like(0xBADF00D)),
    ] {
        let as_f32: Vec<f32> = data.iter().map(|&x| x as f32).collect();
        // realized error of the lossy f32 cast vs the genuine f64 source:
        let mut max = 0.0_f64;
        for (&o, &c) in data.iter().zip(as_f32.iter()) {
            if o == 0.0 {
                continue;
            }
            let e = (((c as f64) - o) / o).abs();
            if e > max {
                max = e;
            }
        }
        let (b32, _, _) = bench_pco(&as_f32);
        let (b64, _, _) = bench_pco(&data);
        let bpv32 = b32 as f64 / as_f32.len() as f64;
        let bpv64 = b64 as f64 / data.len() as f64;
        println!("\n## Regime 2 -- {name} (n={}, genuine f64)", data.len());
        println!("\n| stored as | B/val | max rel err vs f64 source |");
        println!("|---|---|---|");
        println!("| f64 (lossless) | {:.3} | 0 |", bpv64);
        println!("| f32 (lossy cast) | {:.3} | {:.2e} |", bpv32, max);
        println!("(f64/f32 ratio = {:.2}x)", bpv64 / bpv32);
    }
}

/// Load a raw little-endian i64 file (8 bytes per value, no header). Used for
/// the real timestamp columns extracted from exchange L2 captures.
fn load_i64_raw(path: &str) -> Option<Vec<i64>> {
    let raw = std::fs::read(path).ok()?;
    if raw.is_empty() || raw.len() % 8 != 0 {
        return None;
    }
    Some(
        raw.chunks_exact(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect(),
    )
}

/// Encode an i64 column (QuestDB `LONG`) to an in-memory parquet file with the
/// given encoding+codec. The designated-timestamp path uses the SAME i64 delta
/// encoder, so the byte size here matches what QuestDB writes for a timestamp.
/// Returns (file bytes, encode time ms). The size includes the parquet container
/// (footer/schema/page headers); negligible per value at this row count, but it
/// is overhead the bare pco blob does not carry.
fn encode_i64_to_parquet(data: &[i64], encoding_id: i32, codec: &Codec) -> (Vec<u8>, f64) {
    let config = ParquetEncodingConfig::new(encoding_id, codec.id, codec.level).raw();
    let start = Instant::now();
    let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
    let col = Column::from_raw_data(
        0,
        "ts",
        ColumnTypeTag::Long.into_type().code(),
        0,
        data.len(),
        data.as_ptr() as *const u8,
        std::mem::size_of_val(data),
        null(),
        0,
        null(),
        0,
        false,
        false,
        config,
    )
    .expect("column");
    let partition = Partition { table: "bench".to_string(), columns: vec![col] };
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .finish(partition)
        .expect("write parquet");
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    (buf.into_inner(), elapsed)
}

/// Decode the single Int64 column from a parquet file via arrow. Returns
/// (count, decode time ms).
fn decode_i64_from_parquet(bytes: &[u8]) -> (usize, f64) {
    let bytes: Bytes = Bytes::copy_from_slice(bytes);
    let start = Instant::now();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .expect("reader")
        .with_batch_size(65536)
        .build()
        .expect("build reader");
    let mut count = 0usize;
    let mut acc = 0i64;
    for batch in reader.flatten() {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 array");
        count += arr.len();
        if !arr.is_empty() {
            acc = acc
                .wrapping_add(arr.value(0))
                .wrapping_add(arr.value(arr.len() - 1));
        }
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    std::hint::black_box(acc);
    (count, elapsed)
}

/// pco on the first differences: store `[v0, v1-v0, v2-v1, ...]`, pco-compress
/// the whole vector, then reconstruct by cumulative sum and assert exact
/// equality. Tests whether a manual delta beats pco's built-in auto-delta.
/// Returns (blob bytes, encode ms, decode ms).
fn bench_delta_pco(data: &[i64]) -> (usize, f64, f64) {
    let cfg = pco::ChunkConfig::default().with_compression_level(8);
    let mut deltas: Vec<i64> = Vec::with_capacity(data.len());
    if let Some(&first) = data.first() {
        deltas.push(first);
        for w in data.windows(2) {
            deltas.push(w[1].wrapping_sub(w[0]));
        }
    }
    let mut len = 0usize;
    let mut enc_ms = 0.0;
    let mut dec_ms = 0.0;
    for run in 0..RUNS {
        let t0 = Instant::now();
        let bytes = pco::standalone::simple_compress(&deltas, &cfg).expect("pco compress");
        enc_ms += t0.elapsed().as_secs_f64() * 1000.0;
        let t1 = Instant::now();
        let decoded: Vec<i64> = pco::standalone::simple_decompress(&bytes).expect("pco decompress");
        // Reconstruct the original series from the deltas.
        let mut acc = 0i64;
        let restored: Vec<i64> = decoded
            .iter()
            .map(|&d| {
                acc = acc.wrapping_add(d);
                acc
            })
            .collect();
        dec_ms += t1.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(restored, data, "delta+pco round trip");
        std::hint::black_box(&restored);
        if run == 0 {
            len = bytes.len();
        }
    }
    (len, enc_ms / RUNS as f64, dec_ms / RUNS as f64)
}

/// Compare encodings on a single i64 timestamp column.
fn run_timestamp_dataset(title: &str, data: &[i64]) {
    let n = data.len();
    let deltas: Vec<i64> = data.windows(2).map(|w| w[1] - w[0]).collect();
    let zero = deltas.iter().filter(|&&d| d == 0).count();
    let dmin = deltas.iter().copied().min().unwrap_or(0);
    let dmax = deltas.iter().copied().max().unwrap_or(0);
    println!("\n## {title}");
    println!(
        "rows={n}, delta min/max={dmin}/{dmax}, zero-deltas={zero} ({:.1}%)",
        100.0 * zero as f64 / deltas.len().max(1) as f64
    );
    println!("\n| method | bytes/value | ratio vs 8.0 | total bytes | encode ms | decode ms |");
    println!("|---|---|---|---|---|---|");

    let uncompressed = &CODECS[0];
    let zstd = CODECS.iter().find(|c| c.name == "zstd").unwrap();

    // Shipped baseline: DELTA_BINARY_PACKED (encoding id 4), uncompressed and +zstd.
    for (label, codec) in [
        ("DELTA_BINARY_PACKED", uncompressed),
        ("DELTA_BINARY_PACKED+zstd", zstd),
    ] {
        let mut len = 0usize;
        let mut enc = 0.0;
        let mut dec = 0.0;
        for run in 0..RUNS {
            let (bytes, e) = encode_i64_to_parquet(data, 4, codec);
            let (cnt, d) = decode_i64_from_parquet(&bytes);
            assert_eq!(cnt, n);
            enc += e;
            dec += d;
            if run == 0 {
                len = bytes.len();
            }
        }
        report_ts_row(label, len, n, enc / RUNS as f64, dec / RUNS as f64);
    }

    // pco (auto-delta) and manual delta+pco. pco blobs carry no container.
    let (plen, penc, pdec) = bench_pco(data);
    report_ts_row("pco (auto-delta)", plen, n, penc, pdec);
    let (dlen, denc, ddec) = bench_delta_pco(data);
    report_ts_row("delta + pco", dlen, n, denc, ddec);
}

fn report_ts_row(label: &str, total: usize, n: usize, enc_ms: f64, dec_ms: f64) {
    let bpv = total as f64 / n as f64;
    println!(
        "| {label} | {:.4} | {:.2}x | {} | {:.1} | {:.1} |",
        bpv,
        8.0 / bpv,
        total,
        enc_ms,
        dec_ms
    );
}

#[test]
#[ignore = "benchmark; needs /tmp/real_ts_*.i64; run in release with --ignored --nocapture"]
fn run_pco_timestamp_bench() {
    println!("\n# Timestamp codecs: shipped DELTA_BINARY_PACKED vs pco vs delta+pco");
    println!("Real exchange L2 timestamps (i64). pco/delta+pco are bare blobs;");
    println!("DELTA_BINARY_PACKED rows include the parquet container. {RUNS} runs averaged.");
    let mut files: Vec<String> = std::fs::read_dir("/tmp")
        .expect("read /tmp")
        .flatten()
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| n.starts_with("real_ts_") && n.ends_with(".i64"))
        .map(|n| format!("/tmp/{n}"))
        .collect();
    files.sort();
    if files.is_empty() {
        println!("(no /tmp/real_ts_*.i64 files found; skipping)");
        return;
    }
    for f in files {
        match load_i64_raw(&f) {
            Some(data) if data.len() > 1 => run_timestamp_dataset(&f, &data),
            _ => println!("\n## {f}  (could not load; skipped)"),
        }
    }
}
