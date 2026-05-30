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

use arrow::array::Float64Array;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use qdb_core::col_type::ColumnTypeTag;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::parquet::tests::ColumnTypeTagExt;
use crate::parquet_write::file::ParquetWriter;
use crate::parquet_write::lossy::round_f64;
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
    }
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
