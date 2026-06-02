# `export-lp` cost model and ordering

Reference for how `influxd inspect export-lp` actually reads data, how cheap its
fixed startup really is, and why `--start/--end` windows output without pruning
shard reads. The whole chunked-import design (`import_batmon_chunked.sh`,
`tsm_chunk_plan.py`) follows from these facts — though, as the measurements
below show, it is the *sort* that the chunking bounds, not the export.

Verified against the InfluxDB source at tag `v2.7.11`:
`cmd/influxd/inspect/export_lp/export_lp.go`. The glob / `OverlapsTimeRange` /
per-point-filter structure has been stable across the 2.x line. The timing
numbers below are measurements on the bench box (`tm.fabi.me`, batmon bucket
`21bb6302e4d8bc07`, 19 TSM files across 16 shards), not anything the source
pins down.

## TL;DR

- **Output is series-major, not time-ordered.** For each series (one field of a
  tagset) `export-lp` emits that series' entire history ascending, then moves to
  the next series and starts over from its earliest point. The first line is the
  first point of the *first series*, not the globally earliest point. This is why
  the pipeline must `sort -k1,1n` before the pivot, and why the pivot aborts on
  unsorted input.
- **The fixed startup cost is small; the cost is decode-bound.** Every call globs
  and opens *every* TSM file across *every* shard and parses each file's index,
  regardless of `--start/--end` — but for this bucket that fixed cost is only
  **~1 s** (a full-bucket export restricted to an empty desert window returned 0
  lines in **0.98 s**). What actually costs time is decoding the blocks that
  overlap the window: the dense recent week alone (one 7-day shard, 406 M points)
  took **257 s**. So total time scales with the *volume of data in the window*,
  not with a per-call constant. An earlier comment claiming "~80 s fixed per call
  regardless of window" was wrong and has been corrected.
- **`--start/--end` windows output; it does not prune shard reads.** The only
  I/O it can save is *block decode* for files whose entire span falls outside the
  window. A narrow window sitting inside a wide (compacted) TSM file saves
  nothing: that file is still fully decoded and then filtered point-by-point.
- **`--end` is inclusive.** A point exactly on a chunk boundary appears in two
  ranges; DEDUP UPSERT KEYS on the tables make that idempotent.
- **There is no per-shard dump in v2** (see the section below). It would not help
  here anyway: it would save only the ~1 s fixed cost and would *worsen* the sort.

## The three stages, and where the time filter bites

### 1. File enumeration — never pruned (`export_lp.go:209-225`)

```go
tsmPattern := filepath.Join(tsmDir, "*", "*", "*."+tsm1.TSMFileExtension)
tsmFiles, err := filepath.Glob(tsmPattern)   // ALL shards, ALL tsm files
...
sort.Strings(tsmFiles)                         // global filename sort
for _, f := range tsmFiles { exportTSM(f, ...) }
```

Globs `<engine>/data/<bucket>/*/*/*.tsm` — every shard directory, every TSM file
— then the same again for WAL files under `<engine>/wal/...`. Nothing here looks
at `--start/--end`.

### 2. Per-file open + index read — never pruned (`:236-254`)

```go
f, err := os.Open(tsmFile)
reader, err := tsm1.NewTSMReader(f)   // reads the file's index/footer into memory
...
if !reader.OverlapsTimeRange(filters.start, filters.end) {
    return nil                         // file-level skip, but AFTER open+index
}
```

Every TSM file is opened and its index parsed *before* the window is consulted.
`OverlapsTimeRange` skips a file only when its whole span misses the window — and
even then only after paying the open + index cost. This open-every-file +
parse-every-index work is the fixed per-call cost — measured at ~1 s for this
bucket's 19 files, so small relative to decode (stage 3).

### 3. Inside an overlapping file — no block pruning, per-point filter (`:258-282`, `:383-387`)

```go
for i := 0; i < reader.KeyCount(); i++ {
    values, err := reader.ReadAll(key)   // decodes the ENTIRE series in this file
    ...
    writeValues(key, field, values, ...) // then filters point-by-point:
}
// in writeValues:
if ts < filters.start || ts > filters.end { continue }   // ts == end is KEPT
```

For any overlapping file it `ReadAll`s every series in full and discards
out-of-window points one at a time. There is no block-level skip inside an
overlapping file, and `ts > end` (not `>=`) is what makes `--end` inclusive.

## Why this shapes the chunked import

The chunking exists to bound the **sort**, not the export. The pivot needs
globally time-sorted input, and a single global sort of the whole bucket spills
60-90 GB — more than the sort-temp volume holds, which is what OOM-killed an
early run. So the populated span is cut into chunks each small enough to sort in
RAM/disk, fed ascending.

- The *unit* of chunking is compressed bytes, not time: `tsm_chunk_plan.py` reads
  only the TSM *index* (`dump-tsm --index`, no block decode) to histogram
  per-block compressed bytes, then cuts the span into volume-balanced chunks.
  Empty time contributes no bytes, so deserts are absorbed into a neighbouring
  chunk and no export is ever spent purely on an empty span. This keeps every
  chunk's sort bounded regardless of how lumpy the data is in time.
- The per-call export overhead the chunking "wastes" by re-opening the whole
  bucket each chunk is only **~1 s** (measured), not the ~80 s an earlier comment
  claimed. With ~15 chunks that is ~15 s total — negligible next to the decode
  and sort. So the volume-balanced design is justified by the sort bound alone;
  the export fixed cost is in the noise.

## Per-shard dump: missing in v2, and would not help here

A natural idea for a full backfill (no windowing needed) is to dump **one shard
at a time** — open each shard's files exactly once, and exploit that InfluxDB
shards are time-disjoint (a point's shard is chosen by its timestamp and the
shard-group duration, so feeding shards ascending gives ascending appends).
The shard layout confirms the time-disjointness — `autogen` here uses 7-day
shard groups:

```
Shard  Min Time                  Max Time
22     2023-11-08T09:58:55.772Z  2023-11-12T23:59:59.915Z
29     2023-11-13T00:00:00.004Z  2023-11-19T23:59:59.986Z
39     2023-11-20T00:00:00.010Z  2023-11-26T23:59:59.896Z
...
1455   2026-05-25T00:00:00.255Z  2026-05-31T23:59:59.981Z   <- dense recent week
1606   2026-06-01T00:00:00.010Z  2026-06-02T08:47:55.211Z   <- current week
```

**v2 has no tool that dumps a single shard as line protocol:**

- `export-lp` filters only by `--bucket-id`, `--measurement`, `--start`, `--end`
  — there is **no `--shard` flag**, and it always globs the whole bucket
  (`data/<bucket>/*/*/*.tsm`).
- `dump-tsm` *is* per-file, but emits a debug table (the `Pos/MinTime/MaxTime/
  Ofs/Size/Key/Field` rows the planner parses with `--index`), **not** line
  protocol. Re-ingesting it would mean writing a TSM-block to LP converter, i.e.
  reimplementing the back half of `export-lp`.
- `export-index`, `report-tsm`, `verify-*` are diagnostics, not exporters.
- InfluxDB **1.x**'s `influx_inspect export` walked shard dirs, but it is gone in
  2.x and still produced series-major LP needing the same global sort — so it was
  never more efficient on the ordering problem, it just existed.

You *can* synthesize a per-shard export: `export-lp` reads TSM files directly
(`tsm1.NewTSMReader` + `KeyAt`/`ReadAll`, series key pulled from the TSM
composite key) and never opens the series file or TSI index, so pointing
`--engine-path` at a temp dir whose `data/<bucket>/<rp>/<shard>` is a symlink to
one real shard restricts the glob to that shard.

**But the measurements say it is not worth it, and would make things worse:**

- It would save only the **~1 s** fixed re-open cost per chunk. The dominant
  cost is decoding the dense shards (the recent week alone is 406 M points /
  257 s), which a per-shard dump must decode just the same.
- It would *worsen* the sort. The hot week is a single 7-day shard (shard 1455,
  406 M points). The volume-balanced chunker deliberately splits that one shard
  into sub-ranges so each sort fits; a per-shard outer loop would force the whole
  406 M-point shard through one sort — exactly the spill the chunker exists to
  avoid.

So per-shard dumping is a real gap in v2, but for this decode-bound, single-hot-
shard workload the existing volume-balanced `--start/--end` chunker is the better
fit. Per-shard would only pay off on a bucket with many comparably-sized shards
*and* a sort that already fits per shard — not this one.

## Related code

- `import_batmon_chunked.sh` — runs `export-lp --start/--end | sort | pivot` per
  chunk; header comments summarize the boundary/idempotency reasoning.
- `tsm_chunk_plan.py` — index-only, volume-balanced chunk planner.
- `pivot_lp.py` — the downsample pivot; requires globally time-sorted input.

For the downstream side -- how the pivot producer performs, why a DuckDB engine
swap does not help, and how parallelizing it speeds up the backfill -- see
[`backfill-producer-benchmark.md`](backfill-producer-benchmark.md).
