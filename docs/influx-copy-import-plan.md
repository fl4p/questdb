# Fast InfluxDB -> QuestDB importer via CSV + COPY

## Context

The current bulk migration pipeline (`tools/influxdb-migration/`,
`import_batmon.sh`) is:

```
influxd inspect export-lp        # one field per line, series-major
  -> awk-prepend + global sort   # billions of lines, sort -S 1G (BLOCKING, ~100GB)
  -> pivot_lp.py (Python)        # merge fields sharing (tagset, ts) into wide rows
  -> ILP-over-HTTP POST /write   # + _WalThrottle backpressure
  -> QuestDB WAL -> O3 rewrites -> columns
```

This is a **recurring** workload (~100k series, billions of points, live
measurements running to "now"), not a one-time backfill -- so per-run throughput
matters.

**Measured ground truth** (`tools/influxdb-migration/FINDINGS-tm-import-and-pivot-perf.md`,
py-spy on the real tm.fabi.me box):
- The bottleneck is **upstream of QuestDB**. QuestDB ingest has headroom
  (CPU ~43%, WAL lag 0, iowait ~0). The costs are the **single-threaded pivot
  (~300k field-lines/s on one core)** and the **blocking ~100GB global sort**.
- Pivot CPU profile: **`ticking` progress-heartbeat generator = 37.7%** (pure,
  trivially removable overhead); `merge_stream` 27%; schema coercer 16.8% (mostly
  string split, only ~3% real numeric coercion); `_split_lp` 8.5%.

**Key insight.** QuestDB's `COPY` (`ParallelCsvFileImporter`) is purpose-built
for **unordered** files: it scans CSV chunks in parallel, extracts
`(timestamp, offset)` per partition, **sorts each partition index by timestamp in
parallel**, then writes column files directly and attaches the partitions
(`ParallelCsvFileImporter.java:94-104`). Routing the load through COPY:
- **Deletes the upstream global sort entirely** -- better than the FINDINGS doc's
  rec #2 ("merge of presorted runs"): there is no upstream sort at all.
- **Makes parallel pivot embarrassingly parallel** -- removes rec #3's hard
  constraint (must time-shard + concat in order, never series-shard, or O3
  spirals). With COPY, pivot workers emit CSV in ANY order; COPY sorts.
- **Bypasses the ILP feed, the WAL throttle, and O3** -- the goal of rec #4, but
  via a supported text path with none of the "can QuestDB ingest external Parquet
  partitions?" unknowns.

Because QuestDB ingest already has headroom, COPY's ingest-side win is secondary;
its real value is **killing the sort** and **unlocking parallel pivot**.

## Target pipeline

```
influxd inspect export-lp
  -> pivot to wide CSV (per measurement, parallel, any order)   # ticking deleted
  -> COPY (parallel internal per-partition sort, direct column writes)
```

No global external sort. No ILP/WAL/O3. No `_WalThrottle`. One+ CSV file per
measurement staged under `cairo.sql.copy.root`, loaded by COPY into a pre-created
empty partitioned table.

## Design decisions (resolved)

- **Load path:** CSV + COPY for the historical/catch-up bulk. Keep the existing
  ILP-over-HTTP path for small incremental top-ups; dedup the overlap via
  `DEDUP UPSERT KEYS(timestamp, <tags>)`.
- **Recurring + resumable:** target tables declare `DEDUP UPSERT KEYS` so
  resumes/re-runs are idempotent (FINDINGS: a kill can otherwise drop/dupe one
  20s bucket). Support `--start`/resume-from-watermark natively (generalize
  `resume_batmon.sh`) -- incremental catch-up beats reprocessing all history.
- **Pivot mode:** **downsample (regular grid)** is the priority (fields align on
  bucket timestamps -> dense CSV, fewer NULLs; matches the 20s-gridded workload).
  Exact mode stays available.
- **Pivot engine:** ship Python first (lowest risk, reuses proven
  escaping/coercion/downsample), but evaluate a **DuckDB/Polars** pivot engine in
  a bake-off (FINDINGS rec #4) BEFORE any Rust work -- multicore/SIMD/spill, with
  `time_bucket`+`last()`+pivot as a one-liner, emitting CSV straight into COPY.
- **Ordering / global sort:** removed. COPY sorts internally. The pivot output
  need not be time-ordered -- it only needs all fields of the same
  `(tagset, ts/bucket)` merged into one row, which the per-tagset accumulator
  gives (Influx export is series-major / tagset-contiguous; verified Stage 0).

## Implementation stages

### Stage 0 — Free win + verify assumptions (do first)
- **Delete/inline `ticking`** in `pivot_lp.py` (lines ~585-599): fold a local
  `int` counter into `merge_stream`'s loop, drop the generator wrapper. FINDINGS
  measures this at 37.7% self-CPU -> ~1.4-1.5x for ~5 lines, zero risk. Applies
  to BOTH the ILP and CSV paths, so it pays off immediately.
- **Tagset contiguity:** `tools/influxdb-migration/verify_series_major.sh` (new,
  extend `diag_sort.sh`): assert each `(measurement, tagset)`'s field-lines form
  one contiguous run. If yes -> bounded per-series pivot needs no global sort. If
  no -> keep an awk-prepend sort upstream (still get the COPY win).
- **Timestamp format:** push one tiny CSV through COPY on the fork build to
  confirm `timestamp TIMESTAMP_NS` + an ISO-ns `FORMAT`
  (`yyyy-MM-ddTHH:mm:ss.SSSSSSSSSZ`) parses with full ns precision; else fall back
  to an epoch-ns LONG timestamp column.
- **Symbol-merge cost probe (NEW risk specific to COPY):** COPY's
  `symbol_table_merge` phase is single-threaded. The FINDINGS doc's ILP numbers
  don't cover it. Run one real measurement's CSV through COPY and time the
  per-phase split from `sys.text_import_log` to confirm symbol-merge does not
  become the new bottleneck at ~100k-series tag cardinality. Mitigate with
  symbol capacity hints in the pre-created table if needed.

### Stage 1 — Complete-schema table pre-create (`qdb_admin.py`)
COPY validates the CSV header against an existing **empty, partitioned** table.
- Extend `build_create_table_ddl` to emit the **full** column list (designated
  `timestamp TIMESTAMP_NS`, tag columns as `SYMBOL [INDEX]` with capacity hints,
  field columns with declared types, `PARTITION BY <unit>`, `DEDUP UPSERT
  KEYS(timestamp, <tags>)`), driven by the parsed schema file. Reuse
  `parse_schema_columns`. Add a `--full-schema` mode.

### Stage 2 — CSV-emitting pivot (`pivot_lp.py`)
Add a CSV output mode alongside the existing ILP mode. Reuse, do not rewrite:
- `_split_lp` (tolerates spaces in string fields), `bulk_v1.py` LP-escaping-aware
  measurement/tag parsing, `SchemaCoercer` (drop-unknown + coerce), `merge_stream`
  downsample semantics (right-labeled, last-value-in-`[start,end)`).
New CSV behavior:
- One+ output file per measurement under the COPY input root; header = designated
  `timestamp` + field columns + tag columns, column set/order from the schema file
  (no data pre-pass needed).
- **RFC-4180 quoting**: wrap any value containing delimiter/quote/newline in
  `"..."`, double internal `"`. Absent field -> empty cell (NULL).
- Timestamp per Stage 0 outcome. Downsample accumulator keyed by
  `(tagset, bucket)`, flushed on tagset change; output unsorted (COPY sorts).

### Stage 3 — COPY orchestrator (`tools/influxdb-migration/bulk_copy.py`, new)
COPY is **single-flight** -> serialize measurements:
- reuse `bulk_v1.build_export_cmd` / v2 equivalent; pivot per-measurement CSVs
  under `cairo.sql.copy.root` (pipeline: pivot N+1 while COPY loads N),
- per measurement: `qdb_admin` pre-create complete empty table -> issue
  ```sql
  COPY "<db>_<measurement>" FROM '<measurement>.csv'
    WITH HEADER true TIMESTAMP 'timestamp'
         FORMAT 'yyyy-MM-ddTHH:mm:ss.SSSSSSSSSZ'
         PARTITION BY DAY ON ERROR ABORT;
  ```
  via `/exec` -> capture the hex `id`,
- poll `SELECT phase,status,rows_handled,rows_imported,errors,message FROM
  sys.text_import_log WHERE id='<id>' ORDER BY ts DESC LIMIT 1;` -- done on
  `finished`, abort on `failed`,
- delete staged CSV on success; failure truncates the pre-created table (safe
  re-run); `COPY '<id>' CANCEL;` cancels. Reuse `parse_basic_or_token_auth`.
- flags: `ON ERROR` mode (default ABORT), `PARTITION BY`, copy-root, resume
  watermark, parallel-pivot-ahead.

### Stage 4 — Parallel pivot (throughput at scale)
Because COPY removes the ordering constraint, split the export by time-chunk (or
even series) and run N pivot workers writing independent CSVs; COPY ingests them
all. Targets ~N cores vs today's single-core ~300k lines/s. Gate the worker count
on the small-box memory lesson (FINDINGS OOM: never co-schedule heavy sort/export
with a live import; size buffers to host RAM).

### Stage 5 — Runbook + docs
- `import_batmon_copy.sh` (new): COPY analog of `import_batmon.sh` -- no `sort`,
  no `_WalThrottle`. README: COPY path, `cairo.sql.copy.root`/`work.root` on fast
  disk with room for staged CSV + temp index files, worker pool sizing, the
  Stage 0 gates, the global-sort fallback, resume watermark, ILP top-up cutover.

### Bake-off (decide before any Rust) — DuckDB/Polars pivot engine
On one real time-chunk, time `parse -> DuckDB/Polars time_bucket+last()+pivot ->
CSV -> COPY` against `pivot_lp.py (ticking removed) -> CSV -> COPY`. Settle the
two unknowns from FINDINGS rec #4: (a) can the LP parse be vectorized/columnar
rather than scalar; (b) end-to-end vs Python. DuckDB v1.5.3 is on the Mac. Only
fall back to a Rust filter (FINDINGS rec #5) if both Python and DuckDB/Polars
fall short. A Rust pivot is NOT in the default plan.

## Files
- modify: `tools/influxdb-migration/pivot_lp.py` (delete `ticking`; CSV emit mode)
- modify: `tools/influxdb-migration/qdb_admin.py` (complete-schema DDL + DEDUP)
- add: `tools/influxdb-migration/bulk_copy.py` (orchestrator)
- add: `tools/influxdb-migration/verify_series_major.sh` (Stage 0 gate)
- add: `tools/influxdb-migration/import_batmon_copy.sh` (runbook)
- modify: `tools/influxdb-migration/README.md`
- reference (no change): `core/.../text/ParallelCsvFileImporter.java`,
  `CopyImportTask.java`, COPY config in `PropServerConfiguration`.

## Verification (end-to-end)
- **Correctness:** load a bounded real export through both the old ILP path and
  the new COPY path; assert row-count + per-column value parity (string fields
  with spaces/commas, sparse NULLs, ns timestamp fidelity). Cross-check a window
  against the InfluxDB source directly (FINDINGS: "gaps" were real source gaps).
- **Throughput:** time export->pivot->COPY end-to-end on a real measurement;
  record points/s and the per-phase `sys.text_import_log` split (pivot vs COPY
  sort vs symbol merge). This decides parallel-pivot worker count and the
  DuckDB/Polars bake-off outcome.
- **Idempotent resume:** kill mid-run, resume from watermark, confirm
  `DEDUP UPSERT` yields no dupes/drops. Verify COPY truncate-on-failure re-runs
  cleanly and `CANCEL` works.

## Note
Per the in-repo plan convention, on approval I will also save this plan to
`docs/influx-copy-import-plan.md` before starting (plan mode only allows editing
this plan file).
