# pco timestamps — TODO + findings

Tracking follow-up work for the pco codec extension to the i64 family
(LONG/TIMESTAMP/DATE) and the i32 family (SHORT/INT, shipped). See
`docs/lossy-float-compression.md` for shipped behavior and `is_pco_eligible_tag`
(schema.rs) for the eligibility source of truth.

## Open: pco on numeric arrays (DOUBLE[])

- [ ] MEASURE FIRST, then decide. Potentially the biggest win for crypto L2 data
      (order-book vol* level arrays), but a different storage layout (aux offsets
      + flat element data) than the fixed-width scalar path pco lives on today.
      A pco branch would compress the flat element buffer; needs its own
      encode/decode plumbing alongside the existing raw-array encoding, plus a
      marker that survives the `_pm` sidecar. Spike on a real DOUBLE[] column and
      compare pco vs raw-array vs byte_stream_split before committing.

## Open: reduced-precision timestamps (TIMESTAMP_MS, TIMESTAMP_S)

- [ ] GATED ON REAL DATA. Do not implement until measured on the actual column.

The target dataset is **20s-downsampled** = a regular timestamp grid. The L2
finding below already shows pco crushes regular grids to 0.000 bytes/value at
us/ms/s alike (constant delta + int-mult), so TIMESTAMP_MS almost certainly buys
nothing on the timestamp column for this data -- the bytes are in the values
(handled by the shipped SHORT/INT pco work). Re-measure the real ts column when
the data lands, and only then decide between (a) a Parquet-only lossy ms rounding
(round-in-place, decode unchanged -- verified pco-equivalent to a true ms
rescale) and (b) a real ms-precision column type flowing through native .d / WAL /
ILP (much larger; would follow any existing TIMESTAMP_NS precedent).

QuestDB stores TIMESTAMP as microsecond i64; pco auto-deltas and was measured
~2.4x denser than DELTA_BINARY_PACKED+zstd at us precision. Question: does
down-quantizing to ms/s before pco buy meaningfully more density?

### Status of the evaluation (2026-06-01)

The only real timestamp data cached locally (`/tmp/raw_l2.pkl`, exchange L2) is a
**regular 5-second snapshot grid** (median delta = 5_000_000 us, 0.0% sub-second
jitter). pco crushes it to ~0.000 bytes/value at us/ms/s alike, so it cannot
answer the question -- the us/ms/s tradeoff only exists for *irregular,
event-driven* timestamps (raw trades/quotes), which need the remote `cc`/trades
DB to export.

Directional answer from a **synthetic** irregular-timestamp model (Poisson and
bursty arrivals, 2M rows, pco level 8, bytes/value; raw i64 = 8.000):

| regime (mean delta)      |   us  |   ms  |   s   |
|--------------------------|------:|------:|------:|
| quotes fast (~200us)     | 1.139 | 0.092 | 0.001 |
| trades hi-rate (~1ms)    | 1.430 | 0.238 | 0.002 |
| trades hi bursty (~1ms)  | 1.243 | 0.190 | 0.001 |
| trades mid (~50ms)       | 2.136 | 0.890 | 0.036 |
| slow (~1s)               | 2.677 | 1.430 | 0.238 |

- Each decade of resolution dropped saves roughly log2(10) ~ 3.3 bits/value off
  the delta, until it floors at the real event-spacing entropy.
- **us -> ms is the meaningful, safe win: 3-12x smaller**, and sub-ms exchange
  timestamps are usually clock jitter, not signal.
- **us/ms -> s is misleading**: when mean delta < 1s, many events collapse into
  the same second (delta 0), which is lossy in *ordering* and semantics, not just
  precision. Only acceptable when events are genuinely >= 1s apart.
- delta+pco ~ pco auto-delta (manual delta pre-pass earns nothing), consistent
  with the existing `bench_codecs` finding.

### To get the authoritative number on real data

On the box with the trades DB (`cc` env), mirror the `np.save` block in
`.vibe-drops/*log_coding_tr.py` but export the **index** (timestamps), not px/qty:

```python
# d is a per-symbol trades DataFrame with a tz-aware DatetimeIndex
us = (d.index.asi8 // 1000).astype('int64')          # ns -> us (QuestDB TIMESTAMP)
us.tofile(f'/tmp/real_ts_{sym.replace("/","--")}.i64')
(us // 1000).astype('int64').tofile(f'/tmp/real_ts_{sym}_ms.i64')
(us // 1_000_000).astype('int64').tofile(f'/tmp/real_ts_{sym}_s.i64')
```

Then run the shipped-vs-pco comparison (includes DELTA_BINARY_PACKED+zstd):

```
cd core/rust/qdbr && cargo test --release run_pco_timestamp_bench -- --ignored --nocapture
```

## Done: pco on real BMS columns (validates the SHORT/INT feature)

Real BMS cell data (`/tmp/real_bms_*_*.npy`, ~21k rows, f32), lossless
fixed-point, pco level 8, bytes/value (raw f32 = 4.000):

| column  | native f32 pco | centivolt SHORT (x100) | vs native |
|---------|---------------:|-----------------------:|----------:|
| voltage |          1.521 |        1.034 (lossless) | -32%      |
| current |          1.055 |        0.949 (lossless) | -10%      |

- Store BMS voltage as **SHORT centivolts** (lossless to 0.01 V; range
  104.91-113.51 V -> 10491-11351, fits i16): ~3.9x vs raw f32, 32% denser than
  native-f32 pco.
- **SHORT vs INT vs LONG differ by <1%** under pco -- it bins on the value range,
  not the storage width. So the downcast's payoff is the narrower native `.d`
  column (2 bytes vs 8) and i16 fit, not pco density itself.
- millivolt (x1000) overflows i16 (needs INT) and buys nothing in density, so
  **centivolt SHORT is the sweet spot** for this voltage range.
