# InfluxDB v1 HTTP API (Grafana compatibility)

QuestDB exposes an InfluxDB v1-compatible HTTP API so it can act as a drop-in
InfluxDB v1 datasource — most usefully in Grafana, whose InfluxDB v1 datasource
speaks InfluxQL over HTTP.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` / `HEAD` | `/ping` | Health check. Returns `204 No Content` with an `X-Influxdb-Version` header (default `1.8.10`). |
| `GET` / `POST` | `/query` | Runs an InfluxQL statement (`q` parameter) and returns InfluxDB v1 JSON. |
| `POST` | `/write`, `/api/v2/write` | InfluxDB line-protocol ingestion (QuestDB's existing ILP-over-HTTP). |

For `/query`, the InfluxQL text is the `q` URL parameter (GET) or an
`application/x-www-form-urlencoded` body field `q` (POST). Grafana sends
`epoch=ms`; timestamps are returned as integer milliseconds.

## Grafana setup

1. Add a datasource of type **InfluxDB**, Query Language **InfluxQL**.
2. URL: `http://<host>:9000`.
3. Database: the InfluxDB database name. It maps to a QuestDB table-name prefix
   `<db>_` (see [Multiple databases](#multiple-databases)). Leave it empty to
   address tables by their bare name.
4. **Save & Test** — this succeeds on the `/ping` `204` + `X-Influxdb-Version`.

## Multiple databases

The InfluxDB `db` query parameter maps a measurement to the QuestDB table
`<db>_<measurement>`, so several InfluxDB databases can share one QuestDB
instance without measurement-name collisions:

- `?db=mydb` + `SELECT ... FROM "cpu"` resolves table `mydb_cpu`.
- `SHOW MEASUREMENTS` / `SHOW TAG KEYS` / `SHOW FIELD KEYS` / `SHOW TAG VALUES`
  are scoped to that database, and `SHOW MEASUREMENTS` strips the prefix so the
  client sees bare names (`cpu`, not `mydb_cpu`). The `name` in query results is
  the bare measurement too.
- An empty or absent `db` applies no prefix: the measurement is the table name
  directly (single-database setups).

This lines up with the prefix-scoped users in `docs/ACL.md`: a user restricted to
prefix `mydb_` sees exactly the tables of InfluxDB database `mydb`. Choose
database names that form valid QuestDB table-name prefixes.

## Data-model mapping

| InfluxDB | QuestDB |
|---|---|
| measurement | table |
| tag | `SYMBOL` column |
| field | any non-symbol, non-timestamp column |
| `time` | the table's designated timestamp |

## Supported InfluxQL

The subset Grafana's query builder emits:

- `SHOW MEASUREMENTS [WITH MEASUREMENT =~ /regex/] [LIMIT n]`
- `SHOW TAG KEYS [FROM "m"]`
- `SHOW FIELD KEYS [FROM "m"]`
- `SHOW TAG VALUES [FROM "m"] WITH KEY = "k" [WHERE ...]`
- `SHOW RETENTION POLICIES` / `SHOW DATABASES` (synthesized responses)
- `SELECT <agg("field")...> FROM "m" WHERE <tag filters> AND time >= ... AND time <= ... GROUP BY time(<interval>)[, "<tag>"] fill(null|none|previous|0|linear) [ORDER BY time [DESC]]`

Aggregates map as `mean`→`avg`, with `sum`/`count`/`min`/`max`/`first`/`last`
passed through. `GROUP BY time(<interval>)` becomes QuestDB `SAMPLE BY <interval>`
with `ALIGN TO CALENDAR` so buckets align to epoch boundaries like InfluxDB.

## Response format

```json
{"results":[{"statement_id":0,"series":[
  {"name":"cpu","tags":{"host":"h1"},
   "columns":["time","mean"],
   "values":[[1704067200000,1.0],[1704067210000,3.0]]}
]}]}
```

`tags` appears only when grouping by a tag. `columns[0]` is `time` (integer
milliseconds) for SELECT.

## Errors

- A runtime failure for one statement (e.g. unknown measurement) returns
  `HTTP 200` with `{"results":[{"statement_id":N,"error":"..."}]}` so Grafana
  shows it inline.
- A statement that cannot be parsed/translated returns `HTTP 400` with a
  top-level `{"error":"..."}`.

## Configuration

- `line.http.ping.version` — value of the `X-Influxdb-Version` header (default
  `1.8.10`; a v1.x value makes InfluxDB v1 clients select the InfluxQL path).

## Limitations

- Only the Grafana builder subset is translated — no arbitrary raw InfluxQL,
  subqueries, `SLIMIT`/`tz()`, or InfluxDB v2 / Flux (`/api/v2/query`).
- Time-range comparisons are emitted as epoch microseconds, correct for
  microsecond-timestamp tables (the ILP default). Nanosecond designated
  timestamps are not supported for the `WHERE` bound.
- `!~` (regex not-match) is passed through to QuestDB untranslated.
- `SHOW TAG VALUES` value ordering is not guaranteed (Grafana sorts client-side).
- Writes use QuestDB's existing ILP endpoint; InfluxDB v1 `/write` semantics
  (`db`/`rp` params, InfluxDB-style JSON write errors) are not specially handled.
