# InfluxDB → QuestDB migration

Copies **all data** (and, on the HTTP path, **users/permissions**) from an
InfluxDB instance into QuestDB. The table-naming convention
(`<db>_<measurement>`, single underscore) matches the fork's InfluxQL `/query`
endpoint and the file-based ACL prefix contract, so Grafana/REST clients keep
working after the cut-over.

## Pipelines at a glance

There are **three data-migration pipelines**. Pick by how you can reach the
source and how you want to write QuestDB:

| # | Pipeline | Source read | Write to QuestDB | Driver(s) | Engine |
|---|----------|-------------|------------------|-----------|--------|
| 1 | **Offline -> ILP** | `influxd inspect export-lp` (local engine files) | ILP feed (wide pivot) | `import_ha_van.sh`, `import_batmon_chunked.sh` | `pivot_lp.py` |
| 2 | **Offline -> COPY** | `influxd inspect export-lp` (local engine files) | wide CSV + parallel `COPY` | `import_batmon_copy.sh`, `import_batmon_parallel.sh` | `bulk_copy.py` (+ `copy_chunk.py`) |
| 3 | **HTTP -> ILP** | InfluxDB HTTP API (remote, live server) | ILP feed | `influx_migrate.py` | `readers/` + `writer.py` |

- **1 and 2 share one offline front-end** -- `export-lp | sanitize | sort | pivot`
  -- and differ only on the write side. Both need filesystem access to the
  InfluxDB TSM engine directory. ILP (1) goes through the WAL; COPY (2) writes
  column files directly and is idempotent on resume via `DEDUP UPSERT KEYS`.
- **3 is fully separate.** It reads a remote, running InfluxDB over HTTP (no
  engine access) and is the only path that also migrates users/permissions.
- **ACL generation is a post-step, not a pipeline.** `acl_from_artifacts.py`
  consumes the manifest/principals artifacts that pipeline 3 emits and writes
  `conf/acl.conf` (see [`../../docs/ACL.md`](../../docs/ACL.md)); it touches
  neither InfluxDB nor QuestDB data.

The sections below cover the HTTP path (Usage), the shared ILP/schema/index
options, the COPY path, and verification. For the offline `export-lp` pipelines,
`import_ha_van.sh` and `import_batmon_copy.sh` are the ready-to-edit runbooks.

## Install

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt   # influxdb / influxdb-client imported lazily
```

This is only for the **HTTP path** (pipeline 3): install the source client you
need -- `questdb` plus `influxdb` for a v1 source, or `questdb` plus
`influxdb-client` for a v2 source (both imported lazily). The **offline
`export-lp` pipelines** (1 and 2) are pure standard-library Python and shell out
to `influxd inspect export-lp`, so they need nothing installed.

## Usage

Always dry-run first — it reads the source and reports planned tables, row
counts and the `acl.conf`, but writes nothing:

```bash
# InfluxDB v1
./influx_migrate.py --source-type v1 --influx-url http://localhost:8086 \
    --influx-user admin --influx-password secret \
    --questdb-ilp 'http::addr=localhost:9000;' --dry-run

# InfluxDB v2
./influx_migrate.py --source-type v2 --influx-url http://localhost:8086 \
    --influx-token "$INFLUX_TOKEN" --influx-org my-org \
    --questdb-ilp 'http::addr=localhost:9000;' --dry-run
```

Real run, also emitting ACL + a credentials file to hand back to operators:

```bash
./influx_migrate.py --source-type v1 --influx-url http://localhost:8086 \
    --influx-user admin --influx-password secret \
    --questdb-ilp 'http::addr=localhost:9000;' \
    --acl-out conf/acl.conf --credentials-out credentials.csv
```

`--source-type auto` probes `/health` to distinguish v2 from v1.

## Data model mapping

| InfluxDB            | QuestDB                              |
| ------------------- | ------------------------------------ |
| database / bucket   | table-name prefix `<db>_`            |
| measurement         | table `<db>_<measurement>`           |
| tag                 | `SYMBOL` column                      |
| field               | typed column (double/long/str/bool)  |
| time                | designated timestamp                 |

The `/query` endpoint resolves the InfluxDB `?db=<db>` parameter to the
`<db>_` prefix and strips it on output, so Grafana sees bare measurement
names. The separator is a **single underscore** — do not change
`--table-name-template` unless the endpoint changes too. For a single-database
source you can pass `--no-prefix` to use bare measurement names (empty `?db=`).

The prefix is a **literal** `db + "_"` with no transformation. A db/bucket name
that is not already a clean `[A-Za-z0-9_]` identifier is **rejected** — the run
stops with a non-zero exit and names the offender, so it can be renamed upstream
(this guarantees the operator's Grafana `?db=` always equals the table prefix).

## Indexing (opt-in)

ILP auto-create **never adds indexes**, so a freshly imported table has none.
To index a tag you must pre-create the table with that column declared
`SYMBOL INDEX` *before* the first write. The bulk wide-pivot path (`pivot_lp.py`)
does this on request:

```bash
... | pivot_lp.py --prefix batmon_tele_ --downsample 20s \
      --create-table batmon_tele_batmon \
      --index 'did,uid,addrh:2048,slug' \
      --timestamp-type TIMESTAMP --partition-by DAY
```

This runs a `CREATE TABLE IF NOT EXISTS` with the designated timestamp plus the
named indexed columns; every other tag/field column still auto-creates from the
feed. `--index` entries are `col` or `col:capacity` (the SYMBOL index capacity
hint). The shared builder lives in `qdb_admin.py`.

Guidance — **do not index every tag**. A QuestDB `SYMBOL` is already
dictionary-encoded, so a non-indexed equality filter is a cheap vectorized scan
(pruned by the timestamp partition). An index only pays off for a *selective*
filter on a *large* table, and it costs disk plus per-commit maintenance —
expensive for high-cardinality tags. Index only the tags you actually filter or
group by. (InfluxDB indexes the whole tag set by default; QuestDB's model is
different, which is why this is opt-in.)

QuestDB has **no composite (multi-column) index** — each named column gets its
own single-column index. For a multi-tag filter, index the most selective column
and let the rest be scan filters.

Caveat: `CREATE TABLE IF NOT EXISTS` leaves an **existing** table untouched, so
it will not add an index to a table that already lacks one. Either drop and
re-create from the schema, or `ALTER TABLE <t> ALTER COLUMN <c> ADD INDEX`
separately.

## Schema enforcement (opt-in)

When the target tables are pre-created with a tighter schema than the source
(narrower numeric types, BOOLEAN flags, a curated subset of columns), pass that
schema to `pivot_lp.py` so the feed matches it exactly:

```bash
... | pivot_lp.py --prefix batmon_tele_ --downsample 20s \
      --schema-file schema/tables.sql
```

`--schema-file` parses the `CREATE TABLE` statements and, per source
measurement (table name minus `--prefix`):

- **Drops any field not in the schema** so it cannot auto-create a column. This
  is the safe way to keep unwanted columns out. Do **not** instead set
  `line.auto.create.new.columns=false`: that does not silently skip an unknown
  column, it makes the ILP appender reject the **whole row**, so a single stray
  field drops every row that carries it.
- **Coerces each value to the declared column type**: `BOOLEAN` -> `t`/`f`
  (`0`->`f`, nonzero->`t`), `INT`/`LONG`/`SHORT`/`BYTE` -> integer `Ni`,
  `FLOAT`/`DOUBLE` -> float (a stray integer `i` is stripped). This is required
  because ILP will not write a float into a BOOLEAN or INT column -- a type
  mismatch rejects the row, same as an unknown column.
- `SYMBOL`/string columns and the designated timestamp pass through; tags arrive
  in the LP head, not as field tokens, so they are never coerced.

It **warns once per column** (not per row) when a value does not cleanly match
its type -- a non-`0/1` number coerced to BOOLEAN, a fractional value truncated
to INT, or a non-numeric value that gets dropped -- and logs each dropped column
once. The schema parser handles simple `name TYPE [INDEX ...]` column lists; a
type carrying a comma (e.g. `DECIMAL(10,2)`) is not supported.

## Fast bulk load via COPY (`bulk_copy.py`)

For a one-shot historical backfill, `bulk_copy.py` is an alternative to the ILP
path. It pivots the export into one wide **CSV per measurement** and hands each
file to QuestDB's parallel `COPY` (`ParallelCsvFileImporter`), which writes column
files directly. Versus the ILP path it drops:

- **The ILP re-parse, the WAL sequencer, O3 partition rewrites, and the WAL-apply
  throttle.** `COPY` writes columns directly and sorts each partition itself.
- **Idempotent resumes.** Pre-created tables carry `DEDUP UPSERT KEYS(timestamp,
  <tags>)`, so a re-run or resume cannot duplicate rows.

**This is an operational improvement, not a throughput one** — benchmarks (below)
show it is not faster than ILP for this workload. Choose it for the synchronous,
throttle-free, O3-free ingest, not for speed.

**The timestamp sort stays.** `COPY` sorts each *partition* for its own column
writes, but that is not a substitute for the pivot's pre-merge sort: the downsample
pivot merges all fields of a `(tagset, bucket)` into one row using a single open
bucket, which is only correct when the input is timestamp-sorted. The raw export-lp
is series-major (each field's full time-series contiguous), so it **must** be
sorted first — exactly as in `import_batmon_copy.sh`. Verified on the real bucket: sorted
input reproduces the production table exactly; unsorted input produced ~12x too many
fragmented rows, so the pivot now **aborts loudly** on unsorted input rather than
corrupting.

For why `export-lp` is series-major, why its cost is decode-bound (the per-call
fixed cost is ~1 s, not the ~80 s once assumed), why `--start/--end` windows
output without pruning shard reads, and why v2 has no per-shard dump, see
[`export-lp-cost-model.md`](export-lp-cost-model.md) (verified against InfluxDB
`v2.7.11` source and measured on the batmon bucket).

```bash
# v2 bucket piped in, sorted timestamp-major, downsampled to 20s
influxd inspect export-lp --bucket-id B --engine-path /data/engine --output-path - \
  | grep -E '^(batmon|cells),' \
  | awk '{print $NF"\t"$0}' | LC_ALL=C sort -S 1G -k1,1n | cut -f2- \
  | python3 bulk_copy.py --from-stdin --assume-sorted \
      --prefix batmon_tele_ --downsample 20s \
      --schema-file tm-tables.sql --copy-root /var/lib/questdb/import \
      --questdb-url http://localhost:9000 --user admin --password secret
```

The CSVs must be staged **under the server's `cairo.sql.copy.root`** (`COPY`
resolves `FROM` paths relative to it), so run this on the QuestDB host;
`--copy-root` is that local path and `--copy-subdir` the staging directory beneath
it. `COPY` is single-flight, so measurements load serially: the tool pivots the
whole export, then per measurement pre-creates the table (complete, empty,
partitioned), issues `COPY`, polls `sys.text_import_log` to completion, and deletes
the staged CSV. The source can be `--from-stdin`, `--lp-file PATH`, or a built-in
v1 export (`--database`/`--datadir`/`--waldir`). Use `--dry-run` to write the CSVs
and print the `COPY` statements without executing them.

Schema resolution is flexible: columns/types/partitioning come from
`--schema-file` first, and any measurement absent from it falls back to an
**existing table's live schema** read from QuestDB (`--no-schema-from-table`
disables the fallback). Timestamps are written as ISO-8601 nanoseconds and parsed
into a `TIMESTAMP_NS` column via `FORMAT 'yyyy-MM-ddTHH:mm:ss.SSSUUUNNNZ'`; for a
microsecond `TIMESTAMP` column use `--copy-timestamp-format
'yyyy-MM-ddTHH:mm:ss.SSSUUUZ'`.

`import_batmon_copy.sh` is a ready-to-edit runbook (export | grep | awk-prepend
sort | `bulk_copy.py`). `verify_series_major.sh` reports whether an export is
series-major if you want to understand its ordering, but the sort is required
regardless. Keep the ILP path (`pivot_lp.py`) for small incremental top-ups,
deduplicated on the overlap by the table's `DEDUP UPSERT KEYS`.

### Performance (measured on the real batmon bucket)

Both the ILP and COPY paths are **producer-bound** — the single-threaded Python
pivot plus output generation dominates; the ingest is not the bottleneck. End-to-end
on a sorted slice (`bench_copy_vs_ilp.sh`):

| slice | OLD ILP | NEW COPY |
|-------|---------|----------|
| 2h (258k rows) | 7,242 rows/s | 5,930 rows/s (~18% slower) |
| 10h (1.46M rows) | 10,983 rows/s | 7,526 rows/s (~31% slower) |

COPY's *pure* column-write is the fastest ingest measured (~90k rows/s from
`sys.text_import_log`, ~2x ILP; `symbol_table_merge` is a non-issue at 8-15ms), but
generating the wide CSV in Python adds ~70% over the bare pivot and erases that lead,
and the gap grows with scale. ILP's line generation is lighter and, on pre-sorted
input, WAL apply keeps up. So COPY's column-write ceiling only surfaces with a
**compiled producer**.

Measured levers (since the producer is the bottleneck, every lever targets it):

- **PyPy** — running either path under `pypy3` (no code changes) gives ~1.7-1.9x
  end-to-end and narrows the COPY-vs-ILP gap to ~7% (it accelerates the
  string-heavy CSV producer most). Lowest-effort win, and the default in the
  chunked importer.
- **Parallelism** — the producer is single-threaded but the planner's chunks are
  time-disjoint, so producing them is embarrassingly parallel, and COPY is
  O3-free and order-tolerant, so the CSVs can be produced wide and drained
  serially in any order. `import_batmon_parallel.sh` does exactly this: PAR
  producers (`pivot_lp` under PyPy, ~139 MB RSS each) feeding one serial
  `copy_chunk.py` COPY consumer. Measured on the 4-core box while it was *also*
  running QuestDB and a concurrent import (load ~5): K=2 1.49x, K=3 1.73x, K=4
  2.18x aggregate over a single producer; an idle box would scale closer to
  linear. This is the largest lever for fastest total backfill time.
- **DuckDB -> Parquet/CSV** — a DuckDB `time_bucket`+`last()` pivot
  (`duckdb_pivot.py`) is **bit-for-bit equal** to `pivot_lp` output, but at scale
  it is **not faster** than the PyPy `pivot_lp` producer (both are text-parse-bound
  on the LP input) and uses **~14x the RAM**. Measured on a 35.5M-line, 4.3 GB
  sorted day: `pivot_lp`/PyPy 202 s / 139 MB RSS; DuckDB->CSV 206 s / 1.98 GB;
  DuckDB->Parquet 206 s / 1.9 GB. (An earlier "~2-2.5x over ILP" figure compared
  DuckDB against *CPython* ILP on small slices, not against PyPy at scale.) Keep
  `duckdb_pivot.py` as a validated reference, but parallel PyPy is both faster and
  far lighter. Full methodology and numbers: `backfill-producer-benchmark.md`.

## Artifacts (the ACL seam)

Every run writes two machine-readable files so the ACL is generated from what
was actually migrated:

- `--manifest-out migration-manifest.json` —
  `[{influx_db, prefixed: bool, prefix, tables[]}]`. `prefixed` is false for
  `--no-prefix` runs (then `prefix` is `""`); a consumer reads `prefixed`
  rather than inferring from an empty prefix. Doubles as the operator's Grafana
  `?db=` mapping.
- `--principals-out principals.json` — `[{name, is_admin, grants[]}]`, the
  InfluxDB users and permissions.

The canonical `conf/acl.conf` is produced by the ACL tool from these two files,
so there is a single acl.conf writer and prefixes stay consistent. This tool can
also emit `acl.conf` itself with `--emit-acl-conf` (off by default).

## Generate the canonical `acl.conf` (step 2)

`acl_from_artifacts.py` is the canonical, standalone `acl.conf` writer. It reads
the two artifacts from step 1 and never contacts InfluxDB or QuestDB, so you can
(re)generate or adjust the ACL at any time — rotate passwords, change
`--multi-scope-policy` — without re-running the migration.

```bash
# Step 1 — migrate data; emits both artifacts by default on a real run
./influx_migrate.py --source-type v1 --influx-url http://localhost:8086 \
    --influx-user admin --influx-password secret \
    --questdb-ilp 'http::addr=localhost:9000;' \
    --manifest-out migration-manifest.json --principals-out principals.json

# Step 2 — generate the canonical conf/acl.conf from those artifacts
./acl_from_artifacts.py \
    --manifest migration-manifest.json \
    --principals principals.json \
    --acl-out conf/acl.conf \
    --credentials-out credentials.csv
```

`--dry-run` reports the `acl.conf` it would write without touching disk.
`--multi-scope-policy widest|skip|split-note` and `--password-map name,password`
behave exactly as in the opt-in `--emit-acl-conf` path described below. The
prefix for each user is taken **verbatim** from the manifest, so it always
matches the tables the migration actually wrote — including `--no-prefix` runs,
where the prefix is empty. A locked-down/no-grant user still gets a generated
password line, because the fork's ACL loader rejects a user block without one.

## Users & permissions (`--emit-acl-conf`, opt-in)

When you ask this tool to emit `acl.conf` directly, the mapping is:

- Admin / all-access principals → `access=rw`, no prefix (all tables).
- A user scoped to one database → `prefix=<db>_`, `access` from its grant
  (`READ`→`ro`, `WRITE`/`ALL`→`rw`).
- A user with **no** grants → locked-down read-only entry with an unreachable
  prefix (never silently granted all tables).
- A user spanning **multiple** databases — `acl.conf` allows only one prefix +
  one access per username — resolved by `--multi-scope-policy`:
  - `widest` (default): all tables, access = least privilege of the set;
  - `skip`: omit the user;
  - `split-note`: keep the first scope, log the dropped ones.

**Passwords are not migrated.** InfluxDB does not expose existing passwords (v1
stores hashes, v2 uses tokens). The script reuses `--password-map name,password`
when given, otherwise generates strong random passwords and writes them to
`--credentials-out` for redistribution. `acl.conf` stores passwords in
plaintext (as QuestDB already does for `http.user`/PG-wire) — keep it readable
only by the server account.

## Caveats

- **Timestamp precision:** timestamps are read at nanoseconds. Recent QuestDB
  creates a nanosecond timestamp column on first ILP write, so precision is
  preserved; on a build that stores microseconds, sub-microsecond precision is
  lost.
- **Re-runs append.** ILP does not deduplicate, so a second run duplicates rows.
  By default the tool runs a pre-flight check and **refuses** when a target
  table already holds data (exit 3); pass `--allow-duplicate-rows` to override,
  or pre-create tables with `DEDUP UPSERT KEYS(timestamp, <tags>)`. The check
  reuses any `username`/`password`/`token` in `--questdb-ilp`, and is skipped
  for non-HTTP ILP transports (with a warning).
- **Batching:** the ILP `Sender` buffers and auto-flushes; `--batch-size N` sets
  `auto_flush_rows` so a batch flushes every N rows.
- **Large datasets / memory.** Neither reader loads a whole measurement at once:
  the v1 reader keyset-paginates by time (`--page-size`, rows per page); the v2
  reader reads in time windows (`--v2-window-minutes`), one request per window
  with retry on transient drops. Lower `--v2-window-minutes` for dense data: a
  window that is too large makes the InfluxDB **server** compute a huge `pivot`,
  which both blows the client read timeout and can OOM the source server.
  Validated end to end at ~22M rows / ~1.3 GB per source with ~110-120 MB peak
  client RSS on both paths.
- **ILP ingestion bypasses ACL** (it uses the existing ILP auth), so the
  migration itself is unaffected by `acl.conf`; the ACL only governs later
  HTTP/`/query`/REST reads.
- **v2 tokens are not users.** v2 principals are derived from API
  authorizations and named after the token **description** (unique per token;
  the owning user is shared across a user's tokens) — an approximation of the
  v1 per-database user model.

Validated end to end against InfluxDB 1.8 and 2.7.12 -> QuestDB, with
`questdb` 4.1.0, `influxdb` 5.3.2, `influxdb-client` 1.50.0.

## Verify after a run

1. Row-count parity per measurement: InfluxDB `SELECT count(*)` vs QuestDB
   `count(*)` (PG-wire or `/exec`).
2. Read-back through the fork:
   `GET /query?db=<db>&q=SHOW MEASUREMENTS` (expect bare names), then a
   `SELECT mean("f") ... GROUP BY time(1h)` — confirms prefix stripping and
   tag/field classification.
3. ACL: start QuestDB with the generated `conf/acl.conf`; `curl -u user:pass`
   a migrated user against `/query` and confirm prefix scoping + ro/rw, and
   that an out-of-prefix table reports as non-existent.
