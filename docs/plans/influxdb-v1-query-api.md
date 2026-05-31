# InfluxDB v1 HTTP API for QuestDB (Grafana drop-in)

## Context

The goal is to use QuestDB as a drop-in replacement for an InfluxDB v1 datasource
in Grafana. Grafana's InfluxDB v1 datasource talks **InfluxQL over HTTP** to three
endpoints: `/ping`, `/query`, and (for writes) `/write`.

What already exists in QuestDB:
- **`/write`** ingests InfluxDB Line Protocol (`LineHttpProcessorImpl`, paths
  `/write` + `/api/v2/write`). Grafana dashboards are read-only, so this is left
  untouched (decision locked).
- **`/ping`** returns `204 No Content` + an `X-Influxdb-Version` header
  (`LineHttpPingProcessor`), but the default version is `v2.7.4`, which signals
  InfluxDB **v2** and pushes clients onto the Flux path.

The gap: there is **no `/query` endpoint and zero InfluxQL** anywhere in the
codebase. This plan adds a `/query` endpoint that translates the small InfluxQL
subset Grafana's query builder emits into QuestDB SQL, runs it through the
existing `SqlCompiler`/`RecordCursor`, and formats results as InfluxDB v1 JSON
(`results -> series -> {name, tags, columns, values}`) with `epoch=ms` integer
timestamps. It also flips the advertised version to `1.8.10` so Grafana selects
the InfluxQL (v1) query language.

Scope is locked to the **Grafana builder subset** (no general InfluxQL engine,
no raw-query passthrough). Write path unchanged.

## Mapping model

- InfluxDB **measurement** = QuestDB **table**
- InfluxDB **tag** = QuestDB **SYMBOL** column
- InfluxDB **field** = any non-symbol, non-timestamp column
- InfluxDB **`time`** = the table's **designated timestamp** column

## Supported InfluxQL surface (everything Grafana's builder emits)

| InfluxQL | Translation |
|---|---|
| `SHOW MEASUREMENTS [WITH MEASUREMENT =~ /re/] [LIMIT n]` | `SELECT table_name FROM tables()` (+ `WHERE table_name ~ '<re>'`, `LIMIT n`) → series `name:"measurements"`, `columns:["name"]` |
| `SHOW TAG KEYS [FROM "m"]` | `SELECT column AS "tagKey" FROM table_columns('m') WHERE type = 'SYMBOL'` |
| `SHOW FIELD KEYS [FROM "m"]` | `SELECT column AS "fieldKey", <CASE type → influx type> AS "fieldType" FROM table_columns('m') WHERE type != 'SYMBOL' AND designated = false` |
| `SHOW TAG VALUES [FROM "m"] WITH KEY = "k" [WHERE ...] [AND time > now()-<dur>]` | `SELECT DISTINCT '<k>' AS "key", "k" AS "value" FROM "m" [WHERE ...]`; `=~ /re/`→`~ 're'`, `time > now()-5m`→`<tsCol> > dateadd('m',-5,now())` |
| `SHOW RETENTION POLICIES [ON "db"]` | **Synthesized** single row: `autogen, 0s, 0s, 1, true`, columns `["name","duration","shardGroupDuration","replicaN","default"]` |
| `SHOW DATABASES` | **Synthesized** single row, `columns:["name"]`, value = configured db name |
| `SELECT <agg("field")...> FROM "m" WHERE (<tags>) AND time >= Nms AND time <= Nms GROUP BY time(<dur>)[, "<tag>"] fill(...) [ORDER BY time [DESC]]` | `SAMPLE BY` query — see below |

### SELECT → SAMPLE BY rules
- Aggregates: `mean→avg`, `sum/count/min/max/first/last` pass through. Alias each
  output to the influx column name (the bare function name; disambiguate
  collisions as `<func>_<field>`).
- `GROUP BY time(20s)` → `SAMPLE BY 20s`; the `<tsCol>` is selected first
  (becomes `columns[0]`).
- Extra `GROUP BY "tag"` → add `"tag"` as a non-aggregate selected column
  (QuestDB SAMPLE BY groups by selected non-aggregate columns); record it as a
  tag key.
- `fill()`: `null→FILL(NULL)`, `none→FILL(NONE)`, `previous→FILL(PREV)`,
  `0→FILL(0)`, `linear→FILL(LINEAR)`. When Grafana omits fill, emit `FILL(NULL)`
  (matches Influx default, keeps bucket count stable).
- Always append **`ALIGN TO CALENDAR`** so buckets land on epoch-aligned
  boundaries (matches InfluxDB; QuestDB default `ALIGN TO FIRST OBSERVATION`
  would misalign Grafana's axis).
- Time predicate: rewrite literal `time` → `<tsCol>`; convert each `<N>ms` to an
  **ISO-8601 UTC string literal** (`'2024-06-01T00:00:00.000Z'`). String literals
  are unit-safe for both micro and nano designated timestamps, unlike a bare
  micros long.
- **Always inject `ORDER BY <tagKeys...>, <tsCol> [DESC]`** (even if Grafana
  omits it) — the single-pass JSON formatter requires rows grouped by tag tuple
  then time. With `ORDER BY time DESC`, keep tags ASC, time DESC.

### Metadata lookup the translator needs
`<tsCol>` name and which GROUP BY columns are symbols:
```java
TableToken tt = engine.getTableTokenIfExists(measurement);     // null -> per-statement error
try (TableMetadata m = engine.getTableMetadata(tt)) {          // QuietCloseable: close it
    int tsIdx = m.getTimestampIndex();                         // -1 -> "measurement has no time column"
    String tsCol = m.getColumnName(tsIdx);
    // ColumnType.isSymbol(m.getColumnType(idx)) distinguishes tags
}
```
APIs: `CairoEngine.getTableTokenIfExists`, `CairoEngine.getTableMetadata`,
`RecordMetadata.getTimestampIndex/getColumnName/getColumnType`, `ColumnType.isSymbol`.

## Response format

```json
{"results":[{"statement_id":0,"series":[
  {"name":"cpu","tags":{"host":"h1"},
   "columns":["time","mean","max"],
   "values":[[1717200000000,1.2,3.4],[1717200020000,null,5.6]]}
]}]}
```
- `tags` object emitted **only when** grouped by a tag; tag columns are excluded
  from `columns`/`values`.
- `columns[0]` = `"time"` for SELECT; value = `record.getTimestamp(tsIdx)/1000`
  (micros→ms) emitted as a bare integer. (Divide nanos by 1_000_000 for
  `TIMESTAMP_NANO` columns.)
- NULL values → JSON `null` (covers `fill(null)` gaps).
- One series per distinct tag tuple, detected by comparing each row's tag tuple
  to the open series' tuple (works because of the injected ORDER BY).

### Errors
- Per-statement runtime failure (table missing, bad SQL) → HTTP **200**,
  `{"results":[{"statement_id":0,"error":"..."}]}` (Grafana shows it inline).
- Translate/parse failure before any output → HTTP **400**,
  `{"error":"..."}`. Strategy: translate all `;`-separated statements first
  (cheap, no I/O); if any fails and nothing is streamable → 400; otherwise send
  the 200 header and turn per-statement runtime errors into `error` objects.

## New classes (package `io.questdb.cutlass.influxdb`)

- **`InfluxQueryProcessor`** — implements `HttpRequestProcessor`,
  `HttpRequestHandler`, `HttpPostPutProcessor` (POST body `q`), `Closeable`.
  Constructor injects `JsonQueryProcessorConfiguration`, `CairoEngine`,
  `int sharedWorkerCount` (mirror `ExportQueryProcessor`). Override
  `getSupportedRequestTypes()` → `METHOD_GET | METHOD_POST | NON_MULTIPART_REQUEST`.
  Reads `q`/`db`/`epoch` from URL params (GET) or accumulated form body (POST).
  Builds `SqlExecutionContextImpl` like `JsonQueryProcessor`
  (`context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount)`,
  `.with(securityContext, null, null, fd, circuitBreaker.of(fd))`, `initNow()`).
  Holds `LocalValue<InfluxQueryProcessorState>`. Emits `X-Influxdb-Version` on
  responses from `LineHttpProcessorConfiguration.getInfluxPingVersion()`.
- **`InfluxQueryProcessorState`** (LocalValue) — JSON formatter + chunked-send
  resume state machine, modeled on `JsonQueryProcessorState`. Phase enum:
  `RESULTS_PREFIX, STMT_PREFIX, SERIES_OPEN, ROW_PREFIX, ROW_VALUES, ROW_SUFFIX,
  SERIES_CLOSE, STMT_SUFFIX, STMT_ERROR, RESULTS_SUFFIX, DONE`, dispatched via an
  `ObjList<StateResumeAction>`. Persists `factory`, `cursor`, `record`, `phase`,
  `columnIndex`, column-role index lists (`tagColumnIndexes`, `valueColumnIndexes`,
  `timeColumnIndex`), the open series' `currentTagValues`, `seriesName`,
  `statementId`, and statement cursor. Each phase calls `response.bookmark()`
  before writing; `doResumeSend` mirrors `JsonQueryProcessor.doResumeSend`
  (catch `NoSpaceLeftInResponseBufferException` → `resetToBookmark()` +
  `sendChunk(false)`; catch `SqlException`/`ImplicitCastException` → 400).
- **`InfluxQlTranslator`** — hand-written tokenizer + keyword dispatch; returns a
  `TranslatedQuery{ sql, seriesName, tagKeys, valueColumns, timeColumnAlias,
  synth, fill }`. No general grammar.
- **`InfluxQlException`** — translate failure (message + position) for the 400 path.
- (optional) **`InfluxTypeMapping`** — QuestDB `ColumnType` → influx field-type
  string (`float`/`integer`/`boolean`/`string`).

## Existing files to modify

- **`core/.../cutlass/http/HttpFullFatServerConfiguration.java`** — add
  `CONTEXT_PATH_QUERY` ObjHashSet (`add("/query")`) near the other
  `CONTEXT_PATH_*` constants and a default getter `getContextPathQuery()`.
- **`core/.../cutlass/http/HttpServer.java`** — in `addDefaultEndpoints` (after
  the `/exp` `ExportQueryProcessor` bind) add a `server.bind(...)`
  with a `HttpRequestHandlerFactory` whose `getUrls()` returns
  `getContextPathQuery()` and `newInstance()` builds
  `new InfluxQueryProcessor(jsonCfg, cairoEngine, sharedQueryWorkerCount)`. All
  args are already in scope — no signature change, no `Services.java` change.
- **`core/.../cutlass/http/DefaultHttpServerConfiguration.java`** — change the
  ping version default `"v2.7.4"` → `"1.8.10"` so `X-Influxdb-Version` advertises
  v1 (Grafana then uses the InfluxQL path) for both `/ping` and `/query`.

## Reuse (do not reinvent)

- Streaming/resume pattern: `JsonQueryProcessor.doResumeSend` +
  `JsonQueryProcessorState` (bookmark/`NoSpaceLeftInResponseBufferException`,
  phase actions, per-column value writers).
- Constructor/DI + worker-count wiring: `ExportQueryProcessor`.
- 204 + version header template: `LineHttpPingProcessor`.
- SQL functions reused as-is: `tables()`, `table_columns('m')`, `~` regex op,
  `now()`, `dateadd(...)`, `SAMPLE BY ... FILL(...) ALIGN TO CALENDAR`,
  `SELECT DISTINCT`.

## Risks / edge cases

- **Nanos tables**: emit ISO string time literals (unit-safe); divide nanos by
  1e6 when reading time values.
- **No designated timestamp** (`getTimestampIndex()==-1`): SAMPLE BY impossible →
  per-statement error.
- **Series ordering**: formatter is single-pass; correctness depends on the
  injected `ORDER BY <tags...>, time`.
- **Empty result**: emit `"series":[]`, never a half-open series object.
- **fill(none)**: drops empty buckets (irregular timestamps) — default omitted
  fill to `FILL(NULL)`.
- **POST `q`**: parse form-encoded body via `HttpPostPutProcessor.onChunk`;
  still check `getUrlParam("q")` first; reject if neither present.
- **Aggregate alias collisions**: ensure unique `columns` names.
- **Multi-statement `q`** (`SHOW ...; SHOW ...`): split on top-level `;`, one
  `results[]` entry per statement with incrementing `statement_id`; persist the
  statement index across park/resume.
- **Identifier quoting**: QuestDB uses double-quotes for identifiers and
  single-quotes for string literals (same as InfluxQL) — most tag predicates pass
  through; only `=~`/`!~` regex ops and `now()-<dur>` need rewriting.

## Verification

Unit/translator tests (no native memory — plain JUnit):
- `InfluxQlTranslatorTest`: assert each InfluxQL form maps to the expected
  QuestDB SQL string + column roles (SHOW variants, SELECT with/without GROUP BY
  tag, each fill mode, ORDER BY DESC, time-literal→ISO conversion,
  regex/`now()-dur` rewrites).

End-to-end HTTP tests (extend `AbstractBootstrapTest`, drive with `TestHttpClient`
/ `HttpClient`, follow `LineHttpSenderTest` for harness setup; use
`assertMemoryLeak`):
- `/ping` → 204 with `X-Influxdb-Version: 1.8.10`.
- Seed a table via ILP `/write`, then assert `/query` (GET) JSON for:
  `SHOW MEASUREMENTS`, `SHOW TAG KEYS FROM "m"`, `SHOW FIELD KEYS FROM "m"`,
  `SHOW TAG VALUES FROM "m" WITH KEY = "host"`, and a
  `SELECT mean("v") ... GROUP BY time(10s), "host" fill(null)` producing one
  series per host with integer-ms timestamps and a `tags` object.
- POST form-encoded `q` returns the same as GET.
- Error paths: unknown measurement → 200 with per-statement `error`; malformed
  `q` → 400 with top-level `error`.
- Large result (many buckets) to exercise the chunked resume/bookmark path with a
  small response buffer.

Manual smoke test:
- Build (`mvn clean package -DskipTests -P build-web-console,build-binaries`),
  run `ServerMain`, add an InfluxDB (v1) datasource in Grafana pointing at
  `http://localhost:9000`, confirm **Save & Test** passes and a builder-mode
  time-series panel renders against an ILP-ingested table.
