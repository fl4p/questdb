# InfluxDB → QuestDB migration

Copies **all data** and **users/permissions** from an InfluxDB instance (v1.x
or v2.x) into QuestDB. Data is written over InfluxDB Line Protocol (ILP);
users/permissions are replayed into the fork's file-based ACL (`conf/acl.conf`,
see [`../../docs/ACL.md`](../../docs/ACL.md)).

The table-naming convention matches the fork's InfluxQL `/query` endpoint, so
Grafana/REST clients keep working after the cut-over.

## Install

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt   # influxdb / influxdb-client imported lazily
```

Install only the source client you need: `questdb` plus `influxdb` for a v1
source, or `questdb` plus `influxdb-client` for a v2 source.

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

For a one-shot historical backfill, `bulk_copy.py` is the fastest path. Instead of
feeding ILP-over-HTTP, it pivots the export into one wide **CSV per measurement**
and hands each file to QuestDB's parallel `COPY` (`ParallelCsvFileImporter`), which
writes column files directly. Versus the ILP path this drops:

- **The ILP re-parse, the WAL sequencer, O3 partition rewrites, and the WAL-apply
  throttle.** `COPY` writes columns directly and sorts each partition itself.
- **Idempotent resumes.** Pre-created tables carry `DEDUP UPSERT KEYS(timestamp,
  <tags>)`, so a re-run or resume cannot duplicate rows.

**The timestamp sort stays.** `COPY` sorts each *partition* for its own column
writes, but that is not a substitute for the pivot's pre-merge sort: the downsample
pivot merges all fields of a `(tagset, bucket)` into one row using a single open
bucket, which is only correct when the input is timestamp-sorted. The raw export-lp
is series-major (each field's full time-series contiguous), so it **must** be
sorted first — exactly as in `import_batmon.sh`. Verified on the real bucket: sorted
input reproduces the production table exactly; unsorted input produced ~12x too many
fragmented rows, so the pivot now **aborts loudly** on unsorted input rather than
corrupting.

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
