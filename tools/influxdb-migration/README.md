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

- **Timestamp precision:** timestamps are read at nanoseconds; QuestDB stores
  microseconds by default, so sub-microsecond precision is lost unless the
  target column is a nanosecond timestamp.
- **Re-runs append.** ILP does not deduplicate; running twice duplicates rows.
  Pre-create tables with `DEDUP UPSERT KEYS(timestamp, <tags>)` for idempotent
  re-runs.
- **ILP ingestion bypasses ACL** (it uses the existing ILP auth), so the
  migration itself is unaffected by `acl.conf`; the ACL only governs later
  HTTP/`/query`/REST reads.
- **v2 tokens are not users.** v2 principals are derived from API
  authorizations and named after the token user/description — an approximation
  of the v1 per-database user model.

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
