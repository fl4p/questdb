# Backfill producer benchmark: PyPy `pivot_lp` vs DuckDB, and parallel scaling

The InfluxDB->QuestDB backfill is **producer-bound**: the pivot (decode the
series-major export, snap to the downsample grid, widen to one row per
`(series, bucket)`) dominates wall time, not the ingest. So "fastest total
backfill time" is a question about the *producer*. This doc records the
measurements that answer it. The upstream `export-lp` read step (its cost model
and why its output must be sorted) is analyzed separately in
[`export-lp-cost-model.md`](export-lp-cost-model.md).

All numbers measured on the bench box (`tm.fabi.me`, `ubuntu-2gb-hel1-2`: 4
cores, 7.6 GB, ~2.4 GB free, QuestDB running natively) against the real batmon
bucket. Input for the producer comparison: one sorted day of the dense recent
week, **35,506,206 long-format LP lines / 4.3 GB**, materialized with
`export-lp 2026-05-28 | awk-prepend | sort -k1,1n`. Both producers downsample to
20s and emit the same **3,834,459 rows** (9.3x reduction).

## 1. DuckDB does not beat PyPy `pivot_lp`, and costs ~14x the RAM

| producer | output | wall | peak RSS |
|----------|--------|------|----------|
| `pivot_lp.py` under PyPy 3.10 | CSV | **202 s** | **139 MB** |
| `duckdb_pivot.py` (DuckDB CLI 1.5.3, 4 threads, 1.5 GB cap) | CSV | 206 s | 1,983 MB |
| `duckdb_pivot.py` | Parquet (122 MB) | 206 s | 1,904 MB |

DuckDB is ~2% **slower** and uses **~14x the memory**, and the output format does
not matter (CSV and Parquet are within noise) -- so CSV-write was not the DuckDB
bottleneck. Both engines are **text-parse-bound** on the 4.3 GB of LP: reading
and splitting one field-per-line is the dominant cost, and DuckDB's vectorized
`time_bucket`+`last()`+pivot does not recover enough to overtake PyPy's tuned
single-pass merge.

This **corrects** the README's earlier "DuckDB -> Parquet ~2-2.5x" note, which
compared DuckDB against the *CPython* ILP path on small slices. Against the PyPy
`pivot_lp` baseline at scale, DuckDB has no throughput advantage and a large RAM
disadvantage -- decisive on a small box.

`duckdb_pivot.py` is kept as a **validated reference**: on a real 198k-line
sample its output is bit-for-bit equal to `pivot_lp` (28,074/28,074 rows, zero
field mismatches across floats, ints, and the dropped-column allow-list). The one
non-obvious correctness point: line protocol writes integers with a trailing `i`
(`voltage_cell000=3451i`), so the int cast must strip it or every INT column
silently becomes NULL.

## 2. Parallelism is the real lever

The producer is single-threaded, but `tsm_chunk_plan.py` emits **time-disjoint**
chunks, so producing them is embarrassingly parallel. And `COPY` is O3-free and
order-tolerant (`ParallelCsvFileImporter` sorts each partition itself), so the
per-chunk CSVs can be produced wide and drained by a single serial `COPY`
consumer in any order, with no out-of-order partition rewrites.

`pivot_lp` under PyPy holds ~139 MB RSS, so many producers fit the box's free
RAM at once (a DuckDB producer at ~1.9 GB could not run two). Measured aggregate
throughput, running K producers each over an 8.9M-line shard **while the box was
also running QuestDB and a concurrent import (load ~5)**:

| K producers | wall (K x 8.9M) | aggregate | speedup vs K=1 |
|-------------|-----------------|-----------|----------------|
| 1 | 37 s | 240k lines/s | 1.00x |
| 2 | 50 s | 348k lines/s | 1.49x |
| 3 | 65 s | 403k lines/s | 1.73x |
| 4 | 68 s | 522k lines/s | 2.18x |

The knee is ~K=3 on this *contended* 4-core box (QuestDB + a live import already
consumed ~1-1.5 cores during the run). On an idle box with all 4 cores free,
scaling would approach linear. Net: parallel PyPy `pivot_lp` is both faster and
far lighter than a DuckDB engine swap, and it is the largest lever available for
total backfill time.

## 3. The driver

`import_batmon_parallel.sh` implements this: it plans chunks with
`tsm_chunk_plan.py`, produces up to `PAR` chunks concurrently
(`export-lp --start/--end | sort | pivot_lp --csv-out-dir`), and runs a
background drainer that COPYs each chunk as it becomes ready and deletes its
CSVs. `copy_chunk.py` is the serial COPY consumer; it reuses `bulk_copy.py`'s
`build_copy_sql`/`run_copy`/`poll_copy` so the COPY statement, timestamp FORMAT,
and DEDUP table pre-creation are identical to the single-shot path. `PAR`
defaults to 3 (one core left for QuestDB and the serial COPY); raise it toward 4
in a maintenance window.

### Prerequisite and what is not yet verified

- **COPY must be configured.** The driver stages CSVs under the server's
  `cairo.sql.copy.root` and issues `COPY`. On the current `tm.fabi.me` box that
  setting is **not** configured (`/var/lib/questdb/import` is absent), so the
  COPY ingest must be enabled (set `cairo.sql.copy.root`, restart) before the
  driver can run -- the same prerequisite the existing `import_batmon_copy.sh`
  runbook assumes.
- The **produce half** (parallel `pivot_lp`) is measured above. The **COPY
  drain** reuses already-proven `bulk_copy` primitives but was **not**
  end-to-end tested on the live box, because COPY was unconfigured and a
  concurrent import was running -- a collision risk on the production
  `batmon_tele_*` tables. Test it against throwaway `bench_*` tables once COPY
  is enabled.

## Reproduce

```bash
# producer comparison (on the box, after materializing a sorted day to day.lp):
/usr/bin/time -v ~/.local/bin/pypy3 pivot_lp.py --csv-out-dir out/plp \
  --no-csv-schema-from-table --schema-file tm-tables.sql --prefix batmon_tele_ \
  --downsample 20s --csv-timestamp-mode epoch-ns < day.lp
python3 duckdb_pivot.py --input day.lp --schema-file tm-tables.sql \
  --prefix batmon_tele_ --downsample 20s --out-dir out/ddb --format csv \
  --threads 4 --memory-limit 1500MB --temp-dir /path/to/spill --print-sql \
  | /usr/bin/time -v duckdb

# correctness (local): both to CSV, then null-safe keyed value compare.
```
