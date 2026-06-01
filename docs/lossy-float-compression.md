# Lossy float compression for Parquet partitions

## Status

PR 1 (lossless prerequisites: DELTA_BINARY_PACKED default for designated
timestamp, BYTE_STREAM_SPLIT for FLOAT/DOUBLE) is implemented and open as
PR #7189. PR 2 (Tier A mantissa bit-rounding) and the `LOSSY(n)` DDL surface are
implemented locally on the lossy branch, gated on PR #7189 merging first.

The high-ratio internal codec (originally proposed as Tier B, mu-law/LnQ int16)
is now implemented as an integration of the `pco` crate, after the benchmark
below showed it beats both arctic and a hand-rolled int16 codec. `pco` is NOT
the default; it is opt-in via the explicit `PCO` encoding, because a pco column
is not readable by external Parquet tools. `PARQUET(PCO)` stores a FLOAT/DOUBLE
column losslessly with pco; `PARQUET(PCO, LOSSY(n))` rounds first. `LOSSY(n)`
without `PCO` keeps a standard encoding (Plain / BYTE_STREAM_SPLIT) so the file
stays interoperable. pco is implemented end to end: write (round -> pco blob
behind a PLAIN page + a `PcoEncoded` marker), read on both the standalone
`read_parquet()` path and ordinary in-table scans, and three ways to request it
-- at `CREATE`, via `ALTER COLUMN ... SET PARQUET(...)`, and as part of the
per-conversion override `CONVERT ... WITH (lossy = '...')` (which currently uses
the standard encoding). A server config,
`cairo.partition.encoder.parquet.float.encoding`, can make pco (or another
encoding) the default for FLOAT columns without per-column DDL; it defaults to
the standard interoperable layout. See "pco integration" below.

pco also extends to the i64 family (`LONG`/`TIMESTAMP`/`DATE`, the designated
timestamp included), again opt-in via `PARQUET(PCO)` or, for TIMESTAMP, the
`cairo.partition.encoder.parquet.timestamp.encoding` server default. On real
exchange timestamp columns pco is ~2.4x denser than the shipped
DELTA_BINARY_PACKED+zstd at microsecond precision; see "pco on timestamps" below.

pco is also opt-in for the i32-backed integers `SHORT` and `INT` via
`PARQUET(PCO)`. Both serialize through the Parquet INT32 physical type (Parquet
has no INT16): `SHORT` widens i16 -> i32 in the NOT NULL int encoder, `INT` runs
the nullable codec directly with its i32::MIN NULL preserved. pco bins on the
actual value range rather than the storage width, so widening `SHORT` costs next
to nothing in density -- useful when a wide measurement (e.g. millivolt cell
voltages) is down-cast from `LONG` to `SHORT` before conversion. The narrower
and unsigned integers (`BYTE`, `CHAR`, `IPv4`, geohashes) are not yet wired up.

## Summary

Add optional, per-column **lossy** compression for `DOUBLE`/`FLOAT` columns, applied
when a partition is converted to Parquet. The user specifies an accepted error
(a relative tolerance), and the encoder discards numeric precision below that
threshold so the generic Parquet compressor (ZSTD et al.) can shrink the column
far beyond what lossless encoding achieves.

The feature is inspired by the `LnQ16` codec family in the arctic TickStore
(`arctic/tickstore/coding.py`), which logarithmically quantizes financial tick
values to ~16-bit integers at a near-constant relative error (~1 basis point)
across a very large dynamic range.

## Motivation

Financial tick data (prices, quantities) is stored at far higher precision than
anyone queries it at. A price recorded to 15 significant decimal digits is
wasteful when downstream analysis only cares to ~1 basis point (1e-4). The
excess mantissa bits are effectively noise, and noise does not compress: a raw
`DOUBLE` column of prices stored losslessly in Parquet still costs close to its
full 8 bytes per value after ZSTD, because the low-order mantissa bits are
high-entropy.

Discarding precision the user has declared irrelevant turns that noise into
zeros (or into a narrow integer range), which compresses dramatically. The
arctic codec reaches roughly 2 bytes per value at ~1 bp error on real tick
series. This proposal brings the same trade-off to QuestDB without disturbing
its hot-path storage.

## Goals

- Per-column, opt-in lossy compression for `DOUBLE`/`FLOAT`, specified as an
  accepted relative error.
- A closed-form mapping from accepted error to codec parameters, plus a cheap
  calibration step to predict the resulting byte size.
- Correct handling of NULL (the `NaN` sentinel), and of zero / negative values.
- No change to QuestDB's native (`.d`) storage path or its hot-write/random-access
  performance characteristics.
- Round-trippable: a decoded value is guaranteed within the declared tolerance
  of the original.

## Non-goals

- Lossy compression of native `.d` columns. See "Why Parquet only" below.
- Lossy compression of non-floating types.
- Automatic, age-based conversion of partitions to Parquet. That is a separate,
  pre-existing future item (`SqlCompilerImpl.java:4943`).

## Background: current QuestDB storage

QuestDB has two partition storage tiers:

- **Native (`.d` files)** — the default for every partition. `DOUBLE` columns are
  raw IEEE-754 little-endian values, memory-mapped 1:1 and addressed as
  `base + i*8` (`MemoryPARWImpl`, write path `TableWriter.putDouble`,
  `TableWriter.java:13758`). There is **no compression layer** in native storage.
  The format depends on fixed-width random access, which a variable-rate codec
  would break.
- **Parquet partitions** — opt-in, per-partition, via DDL:
  `ALTER TABLE t CONVERT PARTITION TO PARQUET [WHERE ...]` and the inverse
  `CONVERT PARTITION TO NATIVE` (`AlterOperation.java:71-72`,
  `TableWriter.convertPartitionNativeToParquet` ~`1531`). Conversion is manual;
  there is no automatic tiering. The active (latest) partition stays native on
  non-WAL tables; WAL tables may have a Parquet active partition, merged via the
  O3 path. Direct appends to a Parquet partition are not allowed
  (`TableWriter.java:8802`).

### Why Parquet only

Converting a partition to Parquet is already an explicit "freeze this cold data
to save disk, and accept slower access" decision. That is precisely the moment a
user would also accept losing irrelevant precision. Native storage is the speed
tier (fast ingest, O(1) random access, SIMD/JIT execution over raw layout); a
lossy variable-rate codec there would fight everything native is good at, and
native has no compression layer to extend in the first place. So lossy
compression is scoped as an extension of the Parquet conversion path.

## What TickStore does, and how it maps onto QuestDB

TickStore predates having a columnar file format underneath it, so it hand-rolls
three techniques. Two of the three are already provided by Parquet as standard
encodings; only the third is genuinely new.

### 1. Row-masks -> already provided by Parquet nulls

TickStore stores a per-column packed bitmap (`ROWMASK`, lz4-compressed) marking
which rows have a value, so each sparse column stores only its present values
(`tickstore.py:1407`, `rm = ~np.isnan(val)`).

QuestDB's Parquet encoder already does exactly this. A NULL double (the
`Double.NaN` sentinel) maps to a real Parquet null at definition level 0:

- `parquet_write/encoders/numeric.rs:510` — the `f64` encoder's `is_null()` is
  `self.is_nan()`.
- `parquet_write/simd.rs:836` — `encode_f64_def_levels()` records NaN positions
  in the RLE/bit-packed definition-level bitmap.
- `parquet_write/encoders/numeric.rs:316` — null values are filtered out of the
  value buffer entirely.

So an absent value costs ~0 data bytes plus a bit in an RLE bitmap — the rowmask,
standardized. **Do not port it.** (Caveat: the sparse-column win only
materializes if the schema actually leaves untouched fields NULL per row; that is
a data-modeling decision, not a codec one.)

### 2. Index delta-zigzag -> supported, but not the default

TickStore delta-encodes the monotonic timestamp index and varint-packs it
(`tickstore.py:1385`, `np.diff(idx, prepend=0)`).

Parquet's `DELTA_BINARY_PACKED` is exactly delta + zigzag + bit-packing for
integer columns, and QuestDB validates it as legal for the timestamp type
(`schema.rs:549-563`, `Timestamp` is in the accepted set). **But it is not the
default.** `encoding_map` (`schema.rs:691-699`) hands every numeric column
`Plain`:

```rust
match data_type.tag() {
    Symbol                    => RleDictionary,
    Binary | Varchar | String => DeltaLengthByteArray,
    _                         => Plain,   // Long, Int, Timestamp, Double, ...
}
```

So the designated timestamp — a sorted, monotonic microsecond `int64`, about the
most delta-friendly column that exists — is written as raw 8-byte values today.
A user can already opt in by setting that column's encoding to id 4
(`DELTA_BINARY_PACKED`) via the per-column config, but it is never chosen
automatically. **Do not reimplement; change the default.**

### 3. LnQ logarithmic quantization -> genuinely new

Parquet has no lossy float encoding. This is the only piece worth building.

## Design

### Two codec tiers

An IEEE-754 double is already a companded (log-like) representation: exponent
plus mantissa. That gives two natural realizations of "constant relative error,"
at different aggression levels.

**Tier A — mantissa bit-rounding ("bit grooming").** Zero the low mantissa bits,
keeping the top `k`. The value remains a valid IEEE-754 double, so:

- the relative error is bounded by `2^-(k+1)`,
- the Parquet **read path needs zero changes** (it is still a normal double),
- the zeroed low bytes become constant and compress to almost nothing under
  `BYTE_STREAM_SPLIT` + ZSTD.

This is the simple, low-risk 80%. It does not reach arctic's ratios because the
value is still physically a 64-bit double, but it requires no inverse transform.

**Tier B — companded narrow-integer codec (the arctic-style path).** Map each
value through a companding curve, quantize to a narrow integer (int16/int32),
store that integer column in Parquet (with `DELTA_BINARY_PACKED`), and apply the
inverse transform on read. This reaches ~2 bytes per value but changes the
on-disk type and adds a decode step to every read of the column.

For the companding curve, **prefer mu-law over TickStore's raw
`ln(x * 2^prescale + preadd)`**:

```
mu-law:   y = sign(x) * ln(1 + mu*|x|/Xmax) / ln(1 + mu)      (encode, y in [-1,1])
inverse:  x = sign(y) * (Xmax/mu) * ((1+mu)^|y| - 1)          (decode)
```

mu-law improves on the raw-log formulation because:

- It handles **sign and zero natively** (symmetric, smooth through 0), removing
  TickStore's `abs()` + `*sign` + `preadd` workarounds that exist only because
  `ln(0) = -inf`.
- It is **bounded** to `[-1,1]`, eliminating the `prescale` tuning that exists
  only to stop float32 log math overflowing (TickStore overflows at ~2.2e27).
- Its range parameter `Xmax` can be **derived from Parquet per-page min/max
  statistics**, which the encoder already computes — nothing extra to store.

The trade-off to validate: raw log gives constant *relative* error across the
entire range; mu-law gives constant relative error only in its logarithmic
region `|x| >> Xmax/mu` and transitions to constant *absolute* error near zero.
For tick prices this is usually acceptable or even preferable (more robust than a
log blow-up near zero), but the realized error profile must be plotted against a
real price/quantity distribution before declaring it equivalent to the flat-bps
guarantee.

### Sizing the quantizer from an accepted error (closed form)

The quantizer parameters follow directly from the accepted relative error `eps`
and the column's log-dynamic-range `R = ln(xmax/xmin)`; no search is needed.

**Raw log / LnQ.** Stored code `i = round(ln(x) * K)`, `K = 2^n / R`. The
log-domain step is `1/K`, and a half-step error in log space approximates the
relative error, so:

```
rtol ~= R / 2^(n+1)        =>        n = ceil( log2( ln(xmax/xmin) / (2*eps) ) )
```

This reproduces TickStore's own numbers: its `loss` parameter is `2^16/K`, giving
`rtol ~= loss / 2^17` — `loss=15` yields 114 ppm, matching its "LnQ15, 115 ppm"
comment. Worked example: prices spanning `1e-3 .. 1e6` (`R ~= 20.7`) at
`eps = 1 bp = 1e-4` need `n = ceil(log2(20.7 / 2e-4)) = 17` bits per value before
delta and entropy coding.

**mu-law.** Relative-error floor in the log region `~= ln(1+mu) / 2^n`, with the
relative/absolute crossover at `|x| ~= Xmax/mu`. So pick `mu` from the dynamic
range you want kept relative, and `n` from `eps` — two one-line formulas.

**mantissa rounding.** `eps ~= 2^-(k+1)` => `k = ceil(log2(1/(2*eps)))` mantissa
bits kept. Trivial.

### Predicting the byte size (calibration)

The formulas above size the *quantizer* — its entropy ceiling in bits per
sample. They do **not** predict the final file size, because after delta-coding,
ZSTD exploits temporal autocorrelation that is entirely data-dependent. So the
workflow is:

1. Closed-form: pick `n` / `mu` / `loss` / `k` for the error target (instant).
2. Calibration sweep: encode a representative sample at 3-4 settings, measure
   realized rtol and compressed bytes, interpolate. Encoding is fast, so this is
   a seconds-long step and the only empirical part.

## Configuration surface

The accepted error is a per-column property supplied at conversion time. Two
delivery mechanisms, not mutually exclusive:

- **DDL `WITH` clause** on the conversion statement (per-column, explicit):

  ```sql
  ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE timestamp < '2026-01-01'
    WITH (price LOSSY 1bps, size LOSSY 1bps);   -- illustrative syntax
  ```

- **Per-column Parquet config defaults** via `cairo.partition.encoder.parquet.*`
  configuration, for tables converted without an explicit clause.

Both feed the existing per-column config plumbing: `TableColumnMetadata`
`parquetEncodingConfig` (`TableColumnMetadata.java:45`), packed by
`TableUtils.packParquetConfig` (bit layout `TableUtils.java:249`). The packed
i32 currently uses bits 0-25 (encoding, compression, level, explicit flag, bloom
flag — `schema.rs:581-597`); the lossy codec id and a precision parameter need a
home in the spare high bits, or a widening of this config word. This is a
schema/forward-compat decision to settle early because the packed config is
persisted.

## Implementation plan

Three independent, separately shippable pieces, smallest and most reusable first.

### PR 1 — lossless prerequisites (no precision loss)

Self-contained and useful on its own, independent of any lossy work:

- **PR1a (done):** default the designated timestamp to `DELTA_BINARY_PACKED`.
  Implemented as a `default_encoding(data_type, is_designated_timestamp)` helper
  in `to_encodings` (`schema.rs`); non-designated timestamps stay PLAIN since
  they are not guaranteed sorted. The encode dispatch already supported
  `(DeltaBinaryPacked, Timestamp)` (`encode.rs:503-523`), so this was a default
  change only. Covered by Rust unit tests in `schema.rs` and a Java round-trip
  test `PartitionEncoderTest.testDesignatedTimestampDefaultsToDeltaBinaryPacked`.
- **PR1b (done):** implement `BYTE_STREAM_SPLIT` for `Double`/`Float`, both
  encoder and decoder. Encoder transposes the Plain little-endian bytes into K
  per-byte streams (`encoders/numeric.rs` `encode_data`, threaded through
  `encoders/plain/primitive.rs` and dispatched in `encode.rs`); `validate_encoding`
  accepts it for floating types only. Decoder un-transposes the page back to the
  contiguous layout and reuses the existing `PlainPrimitiveDecoder`
  (`parquet_read/decode.rs` `byte_stream_split_to_plain`). Tests: schema
  validation, a self round-trip through our own decoder (Double + Float with NaN
  nulls), and an independent `arrow`-reader round-trip that confirms the bytes are
  spec-compliant rather than merely symmetric with our decoder.

  It was previously *declared*
  (config id 5 parses) but **not implemented**: `validate_encoding` has no arm
  for it and the dispatch in `encode.rs` only matches PLAIN/RleDictionary, so it
  silently falls back to PLAIN (`schema.rs:1057` test confirms). BYTE_STREAM_SPLIT
  is the encoding that makes Tier A's rounded doubles compress well, so it is a
  prerequisite for Tier A and a lossless win regardless.

### PR 2 — Tier A: mantissa bit-rounding

Engine mechanism (done):

- Precision lives in the spare high bits (26-31) of the persisted per-column
  `ParquetEncodingConfig` as "mantissa bits to keep" (0 = no rounding). Read via
  `lossy_keep_bits()`, independent of the explicit flag, so lossy precision
  composes with default encoding/compression. Additive and backward-compatible
  (old configs have 0 in those bits).
- `parquet_write::lossy` (`round_f64`/`round_f32`, byte-buffer variants) does
  round-to-nearest-ties-to-even on the magnitude; NaN (the null sentinel),
  infinity and signed zero pass through unchanged. Max relative error
  `2^-(keep+1)`.
- `encode_column_chunk` rounds `Float`/`Double` column data into owned buffers up
  front when keep-bits are set, so every downstream path (definition levels,
  statistics, value encoding) sees the reduced-precision data. The output is a
  valid double, so the read path is untouched. Composes with `BYTE_STREAM_SPLIT`.
- Tests: scalar and byte-buffer rounding (error bound, ties-to-even, dropped-bit
  clearing, NaN/inf/zero); an end-to-end write-then-read (arrow) confirming
  decoded values equal `round_f64(original, keep)` within the bound.

SQL/DDL surface (done): the precision is set per column inside the existing
`PARQUET(...)` column option as `LOSSY(<keep_bits>)`, where `<keep_bits>` is the
number of mantissa bits to retain (relative error `2^-(keep+1)`). It composes
with the encoding and codec, in any order, and also stands alone:

```sql
CREATE TABLE trades (
  price DOUBLE PARQUET(BYTE_STREAM_SPLIT, ZSTD(9), LOSSY(12)),  -- ~1 bp
  size  DOUBLE PARQUET(LOSSY(10)),                              -- ~5 bp, default encoding/codec
  ts    TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY;
```

`SqlUtil.parseParquetConfig` parses it, validates `LOSSY` is only allowed on
FLOAT/DOUBLE and that keep-bits are in `[1, 52]` (DOUBLE) or `[1, 23]` (FLOAT),
and packs keep-bits into bits 26-31 via `TableUtils.packParquetConfig`. The value
flows through `CreateTableColumnModel` -> `TableColumnMetadata` -> `PartitionEncoder`
-> JNI -> the Rust encoder unchanged, and `SHOW CREATE TABLE` reconstructs the
`LOSSY(...)` clause. Encoding and codec stay orthogonal to precision; there is no
auto-switching of the codec when lossy is enabled (the benchmark recommends ZSTD,
documented, not forced).

Keep-bits chosen rather than a relative-tolerance unit for an unambiguous, exact
mapping to the engine; a tolerance unit (e.g. `LOSSY('1bp')`) could be added later
as sugar over the same field.

### Codec choice (benchmark-driven)

A benchmark over 1M f64 values (price-like random walk and noisy quantity-like
series) through the real pipeline, release build, compared lz4_raw / zstd / gzip /
brotli across rounding levels. Key findings:

- Rounding, not the codec, unlocks compression. BYTE_STREAM_SPLIT alone reaches
  only ~1.1-1.4x because mantissa bits are effectively random; once rounding
  zeroes the low bytes, BSS lines them into runs and ratios jump (price-like at
  keep=10 / ~5e-4 rtol: 16-31x; noisy qty-like: ~3-4x). Realized error stayed
  under the `2^-(keep+1)` bound in every cell.
- zstd is the recommended default: it encodes about as fast as lz4_raw, compresses
  noticeably better, and decodes fastest of the real codecs (fewer bytes to read).
- gzip gives the best ratio (5-20% smaller than zstd) but at a heavy one-time
  encode cost; appropriate only for write-once/read-rarely archival.
- lz4_raw gives up too much ratio; brotli costs more CPU than zstd for no size
  win. Neither is worth surfacing as a lossy preset.

Codec and precision stay orthogonal knobs (no auto-switching the codec when lossy
is enabled). The default codec recommendation maps onto possible named presets
later: `balanced` = zstd, `archive` = gzip.

### Real-data head-to-head vs arctic

A benchmark ran arctic's actual Cython `LnQ16` codecs against several OURS variants
on four real crypto tick columns exported from a TickStore: BCH/USDC (9.1M rows),
FX_BTC_JPY (42.9M), XBTUSD (50M), and BTCUSDT (50M, a contiguous slice of a 918M-row
column spanning 1e-20 to 814084). Both price (positive) and quantity (signed)
columns were tested. The harness lives at `/tmp/arcticbench/bench.py` (not in the
repo); it drives the real `int_coding` Cython module and runs every OURS variant
through the same numpy proxy of the Rust pipeline. The OURS keep-bits knob is
coarser than arctic's continuous `loss` parameter, so matched precision is
approximate -- OURS generally carries slightly more precision than the arctic row
it is compared against. Timings are a single pass (`runs=1`); take them as
order-of-magnitude, not micro-benchmark precision.

Variants measured per column, at each matched precision:

- `OURS bss <comp>-<lvl>` -- round -> BYTE_STREAM_SPLIT -> entropy coder, sweeping
  zstd {1,3,9,19}, gzip {6,9}, lz4 {0,9}, brotli {6,9}.
- `OURS logq` -- round -> log-quantize + delta + zigzag (arctic's transform) ->
  BYTE_STREAM_SPLIT -> zstd-9. Positive-only.
- `OURS xor` / `OURS delta` -- round -> XOR-with-previous / first-difference of the
  bit pattern -> BYTE_STREAM_SPLIT -> zstd-9.
- `pcodec` -- round -> pcodec (a fitted numeric codec, Apache-2.0 Rust crate `pco`),
  level 8. Lossless on the rounded array; degrades gracefully on all inputs.

Three conclusions hold across every dataset.

**1. The entropy coder is not the lever.** Sweeping zstd up to level 19, gzip 9,
and brotli 9 buys only ~20-30% over zstd-1, at 10-100x the encode cost (zstd-19 hit
197 s on a 50M column; gzip-9 142 s). None of them close the gap to arctic. lz4 is
strictly worse on ratio. So a stronger general-purpose compressor is a dead end --
keep zstd at a low level for the standard-Parquet path.

**2. arctic's advantage is the transform, and a fitted codec captures it.** On lossy
price columns -- the realistic case -- `logq` (arctic's own log transform under our
back end) and especially `pcodec` beat arctic on ratio while encoding far faster.
Bytes per value, encode ms in parens:

| Dataset | precision | arctic | best `bss` | `logq` zstd-9 | pcodec |
|---|---|---|---|---|---|
| BCH px | ~190 ppm | 0.222 (1196) | 0.242 zstd-19 (11451) | 0.196 (511) | 0.177 (200) |
| BCH px | ~120 ppm | 0.349 (3062) | 0.343 zstd-19 (11939) | 0.276 (636) | 0.262 (206) |
| FX px | ~190 ppm | 0.152 (3531) | 0.165 zstd-19 (37300) | 0.132 (2062) | 0.123 (788) |
| FX px | ~120 ppm | 0.261 (17090) | 0.252 zstd-19 (57090) | 0.202 (2218) | 0.192 (919) |
| XBTUSD px | ~15 ppm | 0.357 (9355) | 0.438 zstd-19 (58058) | 0.342 (3063) | 0.334 (933) |
| XBTUSD px | ~190 ppm | 0.097 (2267) | 0.106 zstd-19 (32464) | 0.083 (1739) | 0.076 (754) |
| XBTUSD px | ~120 ppm | 0.173 (12874) | 0.166 zstd-19 (38441) | 0.132 (2738) | 0.123 (1124) |
| XBTUSD qty (signed) | ~120 ppm | 1.164 (16539) | 1.224 zstd-19 (197814) | N/A | 0.870 (1576) |

On these lossy points pcodec is 1.07-1.41x smaller than arctic and encodes 3-18x
faster (decode 4-10x faster). `logq` trails pcodec but also beats arctic, which is
the direct proof that the transform -- not the entropy stage -- was the gap.

**3. arctic still wins on lossless / coarse-integer columns, but is fragile.** When
data sits on an integer grid and is stored losslessly, arctic's delta+varint wins
ratio; pcodec cannot match it there, though it stays 5-16x faster:

| Dataset | arctic | `logq` | pcodec | winner |
|---|---|---|---|---|
| BCH px lossless | 0.573 | 1.078 | 1.191 | arctic 1.85x |
| FX px lossless | 0.487 | 0.865 | 0.882 | arctic 1.80x |
| BTCUSDT px (1e-20 - 814k) | 0.074 | 0.148 | 0.101 | arctic 1.36x |
| BCH qty lossless (signed) | 2.005 | N/A | 2.666 | arctic 1.33x |
| FX qty lossless (signed) | 1.663 | N/A | 2.044 | arctic 1.23x |
| BTCUSDT qty lossless (signed) | 1.188 | N/A | 1.398 | arctic 1.18x |

But arctic's ratio comes with a robustness cost. On BTCUSDT px (1e-20 to 814084),
only `LnQ185` survived (0.074, lossless); `LnQ25`/`LnQ15` silently produced
774-885% max relative error (the log-quantizer overflows int16) and the signed
`LnQ15gz` crashed outright (`1e-20 maps to -40241 < 0`). Both arctic and pcodec also
emit a proprietary blob, not a standard Parquet encoding -- so files written that
way are readable only by the writer, not by external Parquet tools.

Design implication. Two regimes:

- Standard-Parquet path (files must stay readable by external tools): confined to
  standard encodings, so `round -> BYTE_STREAM_SPLIT -> zstd` is the ceiling. That
  is what PR 2 ships. It gives up 1.3-2.9x of ratio versus arctic on smooth/coarse
  data in exchange for bounded error, valid IEEE-754, fast encode, and robustness.
- QuestDB-internal path (Parquet files never leave QuestDB): `round -> pcodec` is
  the stronger option -- smaller than arctic on the realistic lossy case, 3-18x
  faster, and robust where arctic's log codecs fail. Adopting it means vetting the
  `pco` crate as a `qdbr` dependency, and confirming it returns `Result`/`Option`
  (never panics) on malformed input, since a panic across JNI aborts the JVM. This
  supersedes the parked Tier B (an LnQ-style int16 codec): pcodec gets arctic-class
  ratios on lossy data without inheriting the log quantizer's failure modes.

Confirmed through the real encoder. The proxy numbers above were re-measured with
the real `pco` crate against the real QuestDB Parquet writer/reader
(`bench_codecs::run_pco_real_bench`, f32 columns), and match to the third decimal.
Lossless `pco` (no rounding) beats round-trip BSS+zstd on every real column --
BCH px 1.191 vs 1.963 (1.65x), BTCUSDT px 0.101 vs 0.244 (2.4x), FX px 0.882 vs
1.798 (2.0x), XBTUSD qty 0.878 vs 1.498 (1.7x) bytes/value -- by 1.16-2.4x, and
decodes ~1.5-2x faster. Under rounding it wins by 1.3-2.1x almost everywhere; the
sole exception is the extreme-range BTCUSDT px column under aggressive rounding
(keep=11: BSS+zstd 0.021 vs pco 0.030), where rounding collapses the near-constant
column into long runs that zstd's LZ stage compresses better than pco's per-value
model. pco encode runs ~1.3-1.6x slower than zstd level 1. So pco is a strong
opt-in for both lossy and lossless float columns -- but because its output is
not standard Parquet, it is offered behind the explicit `PCO` encoding rather
than as the default; BSS+zstd remains the interoperable default.

f32 vs f64 density under pco. pco charges for information content, not the
declared width (`bench_codecs::run_pco_f32_vs_f64_bench`). For values that are
exactly f32-representable, storing them as f32 or as f64 costs the same to
within ~0.1% at every precision (the widened doubles carry all-zero low mantissa
bits, which pco strips):

| keep_bits | f32 B/val | f64 B/val | f64/f32 |
|---|---|---|---|
| full | 1.675 | 1.677 | 1.00x |
| 16 | 0.799 | 0.801 | 1.00x |
| 12 | 0.306 | 0.307 | 1.00x |
| 11 | 0.202 | 0.203 | 1.00x |

For genuine 52-bit doubles (a random walk that fills the mantissa), pco-lossless
costs 5.3 (price-like) to 7.1 (qty-like) bytes/value; the f32 form is 2.0-3.2x
smaller, but only because casting to f32 is itself lossy (discards ~29 mantissa
bits, ~1e-7 relative error) -- not a free win. So under pco, FLOAT vs DOUBLE is a
precision decision, not a storage lever; this is why the server-config default
above is scoped to FLOAT.

### pco on timestamps (real-data benchmark)

`bench_codecs::run_pco_timestamp_bench` compares the shipped designated-timestamp
encoding (DELTA_BINARY_PACKED, with and without zstd) against pco and against a
manual delta-then-pco, on real exchange timestamp columns (i64). The datasets span
three regimes from a BitMEX L2 capture and a regular-cadence BTC quote feed:
bursty (93% duplicate timestamps within the same millisecond), sparse/irregular
(3 years, distinct timestamps, gaps up to 117 days), and a perfectly regular 1 s
grid. Bytes per value (pco/delta+pco are bare blobs; the DELTA rows include the
parquet container, negligible on the large datasets):

| dataset | regime | unit | DELTA+zstd | pco | pco advantage |
|---|---|---|---|---|---|
| XBTUSD 3-year (10.5M rows) | sparse/irregular | us | 1.344 | 0.557 | 2.4x |
| XBTUSD L2 (199k rows) | bursty, 93% dup | us | 0.246 | 0.101 | 2.4x |
| XBTUSD L2 (199k rows) | bursty, 93% dup | ms | 0.163 | 0.101 | 1.6x |
| BTC quotes (21k rows) | regular 1 s grid | us | 0.018 | 0.003 | pco wins* |

(*at 21k rows the DELTA row is dominated by parquet container overhead, so read
the regular-grid line as "pco still smaller", not a reliable multiple.)

Three findings, robust across the regimes:

- **pco beats the shipped DELTA_BINARY_PACKED+zstd on every real regime**, by
  1.6-2.4x at microsecond precision and more on the regular grid.
- **pco is insensitive to the microsecond scaling that hurts DELTA.** The same
  series as ms vs us: pco stays 0.101 both ways, while DELTA+zstd grows from 0.163
  to 0.246 (DELTA bit-packs the larger us deltas; pco models the x1000 factor).
  QuestDB stores designated timestamps in us, so pco's edge is structural.
- **A manual delta-then-pco earns nothing over pco's built-in auto-delta**
  (0.557 vs 0.567; 0.101 vs 0.167). The decoder does not implement a delta+pco
  path; plain pco is the only i64 pco form.

Costs, weighed equally: pco encodes ~2.5x slower than DELTA+zstd (184 ms vs 74 ms
on the 10.5M-row column; decode is comparable or faster), which is acceptable for
a conversion-time codec but real. A pco timestamp column is not readable by
external Parquet tools (same interop tradeoff as float pco), which is why it is
opt-in. The datasets are one instrument (XBTUSD) across three distributions plus
one quote grid; the distributions differ sharply, but cross-venue generality is
not yet established.

### Enabling pco

Both paths only take effect when a partition is converted to Parquet (pco is a
Parquet-conversion codec, not a native-storage one).

Per-column at `CREATE TABLE` (explicit, persisted to column metadata):

```sql
CREATE TABLE trades (
  price  DOUBLE  PARQUET(PCO),    -- pco on a DOUBLE
  qty    LONG    PARQUET(PCO),    -- pco on a LONG
  ts     TIMESTAMP PARQUET(PCO)   -- pco on the designated timestamp
) TIMESTAMP(ts) PARTITION BY DAY;

-- or set it on an existing column:
ALTER TABLE trades ALTER COLUMN ts SET PARQUET(PCO);

-- conversion is when the encoding is actually applied:
ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts < '2026-01-01';
```

Server default (applies to columns with no explicit `PARQUET(...)`; not persisted,
resolved at conversion time, an explicit per-column setting always wins):

```properties
# conf/server.conf (or the QDB_... env var)
cairo.partition.encoder.parquet.float.encoding=pco       # FLOAT and DOUBLE columns
cairo.partition.encoder.parquet.int.encoding=pco         # SHORT, INT, LONG columns
cairo.partition.encoder.parquet.timestamp.encoding=pco   # TIMESTAMP and DATE columns
```

Each knob accepts `default` (the encoder's own choice -- the standard, externally
readable layout; the designated timestamp's `default` is DELTA_BINARY_PACKED),
`plain`, `pco`, plus `byte_stream_split`/`bss` (float family) or
`delta_binary_packed` (int/timestamp family). The three families cover every
pco-eligible scalar type that has a server default. `DECIMAL32`/`DECIMAL64` are
also pco-eligible but have no server knob, so they need per-column `PARQUET(PCO)`
(pco compresses their native unscaled integer, bypassing the big-endian FLBA
layout). `DECIMAL8`/`16`/`128`/`256` are not eligible.
Confirm a conversion took with `table_partitions('trades')` (`isParquet`,
`parquetFileSize`): a pco column is far smaller than the same data stored plain.

### pco integration (implemented)

The original plan here was Tier B, a mu-law / LnQ companded int16/int32 codec.
The real-data benchmark redirected it to the `pco` crate, which reaches
arctic-class (and on lossy data, better-than-arctic) ratios without the log
quantizer's failure modes; the `logq` result showed a hand-rolled int16 codec
would at best match `pco` with more risk. pco is implemented and opt-in.

Surface (FLOAT/DOUBLE and the i64 family LONG/TIMESTAMP/DATE):

- `PARQUET(PCO)` -- lossless pco. Works on FLOAT/DOUBLE and on LONG/TIMESTAMP/DATE
  (the designated timestamp included).
- `PARQUET(PCO, LOSSY(n))` -- round to `n` mantissa bits, then pco. FLOAT/DOUBLE
  only; `LOSSY` is mantissa rounding and is rejected on integer/timestamp types.
- `LOSSY(n)` without `PCO` -- round, then a standard encoding (interoperable).
- `ALTER TABLE t ALTER COLUMN c SET PARQUET(PCO[, LOSSY(n)])` -- set it on an
  existing column.
- `ALTER TABLE t CONVERT PARTITION TO PARQUET ... WITH (lossy = 'c:n, ...')` --
  one-shot rounding for a single conversion (standard encoding; not pco).

The i64 path is mechanical: `pco_codec::compress`/`decompress` are generic over
`pco::data_types::Number`, so `SimdEncodable::encode_pco` for `i64` and the
decoder's `pco_decode_to_value_bytes::<i64>` reuse the same code as f32/f64. A
single helper `schema::is_pco_eligible_tag` is the source of truth for which
column tags may carry pco; the write encoder, both `PcoEncoded` markers (footer
QdbMeta and the `_pm` `ColumnFlags` sidecar), the compression override, and DDL
validation all consult it, so the encode gate and the marker never disagree.

Server-level defaults:

- `cairo.partition.encoder.parquet.float.encoding` selects the default encoding
  for FLOAT and DOUBLE columns during native-to-Parquet conversion. Values:
  `default` (the default -- leaves the choice to the encoder, i.e. the standard
  interoperable layout), `plain`, `byte_stream_split` (alias `bss`), and `pco`.
  Set it to `pco` to make FLOAT/DOUBLE columns default to pco without an explicit
  `PARQUET(PCO)` on every column.
- `cairo.partition.encoder.parquet.int.encoding` is the SHORT/INT/LONG sibling.
  Values: `default`, `plain`, `delta_binary_packed`, and `pco`;
  `byte_stream_split` is rejected (float-only). Set it to `pco` to make the
  integer family default to pco without per-column DDL.
- `cairo.partition.encoder.parquet.timestamp.encoding` is the TIMESTAMP/DATE
  sibling. Values: `default` (the encoder's own default -- DELTA_BINARY_PACKED for
  the designated timestamp, externally readable), `plain`, `delta_binary_packed`,
  and `pco`. `byte_stream_split` is rejected (it is a float-only encoding). Set it
  to `pco` to make TIMESTAMP/DATE columns default to pco without per-column DDL.
- Each default is applied only to columns of its type that carry no explicit
  `PARQUET(...)` encoding; other columns (e.g. DOUBLE, LONG, explicitly-encoded)
  are untouched. The applied default is not persisted to the column metadata --
  it is resolved at conversion time, so changing the config changes future
  conversions only. An encoding not valid for the type is rejected at startup.
  There is no FLOAT default for DOUBLE, and no LONG knob; lossless pco on a
  genuine 52-bit DOUBLE is far less dense than on a FLOAT (see below), and LONG
  columns vary too much to default centrally -- use per-column `PARQUET(PCO)`.

How it works:

- `ParquetEncodingConfig::is_pco()` is true for the explicit `PCO` encoding
  (id 6); `encoding()` maps id 6 to None so the data page header is written as
  PLAIN. The page body holds a pco blob of the present (non-null) values
  (`SimdEncodable::encode_pco`), and the column's Parquet compression is forced
  to Uncompressed (pco already entropy-codes).
- The column is marked `QdbMetaColFormat::PcoEncoded` in the parquet footer
  QdbMeta, and -- crucially for in-table scans -- a `PCO_ENCODED` bit is set in
  the compact `_pm` metadata sidecar's `ColumnFlags`. The standalone
  `read_parquet()` path reads the footer QdbMeta; the in-table page-frame
  decoder reads the `_pm` flag. Both map back to `PcoEncoded`.
- On read, the decoder pco-decompresses the page body back to the present values
  and feeds them through the normal PLAIN primitive decoder, so definition
  levels scatter them into the null (NaN) positions. The logical column type is
  unchanged.

Limitations / follow-ups:

- A pco column is not readable by external Parquet tools; that is the reason pco
  is opt-in rather than the default.
- Predicate pushdown / page-stats pruning over pco columns is not specialized;
  pages decode fully. Worth measuring before relying on pruning for pco columns.
- The `CONVERT ... WITH (lossy=...)` override currently always uses the standard
  encoding; a `WITH (pco=...)` variant could be added if a one-shot pco
  conversion is wanted.

## Read path

- Tier A requires no read changes: the stored value is a normal double already
  within tolerance.
- Tier B requires the Parquet reader to recognize the companded column and apply
  the inverse transform, materializing a `DOUBLE` for the rest of the engine. The
  logical column type stays `DOUBLE`; only the physical Parquet representation
  differs. Predicate pushdown / page-stats pruning on these columns must account
  for the transform (min/max in companded space map monotonically to value space,
  so range pruning is still valid, but the comparison must be done correctly).

## Testing

Per repository conventions (`assertMemoryLeak`, error-path resource cleanup):

- Round-trip error bound: for each codec and a range of `eps`, assert every
  decoded value is within the declared tolerance of the original, across the full
  dynamic range including subnormals, very large, and very small magnitudes.
- NULL handling: `NaN` must round-trip as a Parquet null (Tier A) or be excluded
  from companding and restored as NULL (Tier B). Confirm null doubles still cost
  ~0 data bytes.
- Sign and zero: signed inputs and exact zeros for Tier B (mu-law) round-trip
  correctly.
- Non-finite guard: `+inf`/`-inf` inputs must fail fast or be treated as NULL,
  never silently corrupt (cf. TickStore's `_assert_finite`).
- Resource cleanup on every error path in the encoder and the
  convert-to-Parquet / convert-back-to-native flows.
- Convert-to-Parquet-then-back-to-native: define and test the semantics —
  conversion back to native cannot recover lost precision; the rounded values are
  what persist.
- Calibration: a test asserting the closed-form `rtol` formula matches measured
  round-trip error within a small margin for each codec.

## Risks and trade-offs

- **Irreversible precision loss.** Once a partition is converted with a lossy
  codec, the original precision is gone; converting back to native restores the
  rounded values, not the originals. This must be explicit in the DDL and
  documented; it is the one genuinely destructive operation here.
- **Tier B read cost.** The inverse transform runs on every read of a companded
  column, trading CPU for the smaller on-disk size. Acceptable for cold archival
  data, but it is a real cost and should be measured, not assumed negligible.
- **mu-law near-zero behaviour.** Constant-relative only in the log region; the
  flat-bps guarantee does not hold for tiny magnitudes. Validate against real
  distributions before claiming arctic-equivalent error.
- **Persisted config format.** The packed per-column config is written to disk;
  the lossy codec id and precision must be laid out with forward compatibility in
  mind, and old files without the field must read back correctly.
- **Interoperability.** Tier A output is plain Parquet readable by any tool.
  Tier B stores integers plus QuestDB-specific transform metadata; external
  readers see integers, not the intended doubles, unless they know the transform.
  This narrows Tier B's interop benefit relative to staying lossless.

## Open questions

- Error specification units in DDL: relative tolerance (`1bps`, `1e-4`),
  kept-significant-bits, or kept-decimal-digits? Relative tolerance maps cleanly
  to all three codecs via the closed-form sizing.
- Should `Xmax` for mu-law be per-page (from Parquet stats, adapts to local
  range) or per-column (one value, simpler, stored once)?
- Is Tier B worth the read-path complexity and interop cost, or does Tier A +
  `BYTE_STREAM_SPLIT` + ZSTD capture enough of the benefit for the target
  workloads? Decide with the calibration sweep on real data before committing to
  PR 3.

## References

- arctic TickStore codecs: `arctic/tickstore/coding.py` (`LnQ16_VQL`, `ln_q16` /
  `e_q16`, `log_q16` / `exp_q16`), rowmask and index encoding in
  `arctic/tickstore/tickstore.py`.
- QuestDB Parquet encoder: `core/rust/qdbr/src/parquet_write/`
  (`schema.rs`, `encode.rs`, `simd.rs`, `encoders/numeric.rs`).
- Parquet config plumbing: `TableColumnMetadata.java`, `TableUtils.java`
  (`packParquetConfig`), `PartitionEncoder.java`, `ParquetEncoding.java`,
  `ParquetCompression.java`.
