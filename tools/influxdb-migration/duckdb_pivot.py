#!/usr/bin/env python3
"""DuckDB time_bucket+last() pivot producer -- a vectorized alternative to the
single-threaded Python ``pivot_lp.py`` for the InfluxDB->QuestDB backfill.

It reads the SAME timestamp-sorted long-format line protocol that ``pivot_lp``
reads (one ``measurement,tags field=value ts`` per line), but does the pivot in
DuckDB SQL: parse -> snap to a fixed downsample grid -> keep the LAST value per
field per ``(series, bucket)`` -> widen to one row per ``(series, bucket)`` ->
write Parquet (or CSV) per measurement. The downsample semantics match
``pivot_lp.merge_stream`` exactly:

  * bucket = floor(ts / interval) * interval  (half-open ``[k*I, (k+1)*I)``)
  * keep the last value of each field in the bucket, by timestamp order
  * stamp the row at the bucket END ``(k+1)*I``  (right-labeled, no look-ahead)

The schema file is the column allow-list, exactly as in the CSV path: only the
columns declared for a measurement are emitted; any other source field (e.g. the
247 dropped ``temperatures_*``) is parsed but not written. Types are coerced to
the declared QuestDB types (FLOAT/INT/LONG -> numeric, BOOLEAN <- 0/1).

Output is Parquet by default (loaded into a pre-created partitioned WAL table via
``INSERT INTO t SELECT * FROM read_parquet(...)``) or CSV (``--format csv``, for
the O3-free ``COPY`` path). Unlike the Python pivot this does NOT require the
single-open-bucket invariant, because DuckDB groups explicitly -- but feeding it
the same per-chunk sorted stream keeps peak memory bounded to one chunk.

This is a benchmark/prototype: it measures whether DuckDB's vectorized pivot
beats the PyPy ``pivot_lp`` producer at scale and whether its RSS fits a small
box. Tie-breaking among samples sharing an exact timestamp within a bucket is
undefined here (DuckDB) vs stream-order (pivot_lp); on this data exact-ts
duplicates within one field are absent, so the outputs match.
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Dict, List

from qdb_admin import TableSchema, parse_schema_tables

log = logging.getLogger("duckdb_pivot")

# Map a parsed ColumnDef.kind to a DuckDB cast expression over the raw text value
# `v`. try_cast keeps a malformed value from aborting the whole import (it lands
# NULL), matching pivot_lp's per-token coercion which drops bad values.
_INTERVAL_UNITS = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000,
                   "m": 60_000_000_000, "h": 3_600_000_000_000}


def parse_interval_ns(spec: str) -> int:
    """Parse a downsample spec like ``20s`` / ``1m`` into nanoseconds (0 = none)."""
    if not spec:
        return 0
    spec = spec.strip()
    for unit in ("ns", "us", "ms", "s", "m", "h"):
        if spec.endswith(unit):
            num = spec[: -len(unit)]
            return int(num) * _INTERVAL_UNITS[unit]
    # bare number -> seconds
    return int(spec) * _INTERVAL_UNITS["s"]


def _cast_expr(kind: str, value_sql: str) -> str:
    """DuckDB expression coercing the raw text value to the column's kind."""
    if kind == "float":
        return f"try_cast({value_sql} AS DOUBLE)"
    if kind == "int":
        # cells INT, problem_code LONG. Line protocol writes integers with a
        # trailing 'i' (e.g. "3451i"); strip it before casting. Cast via DOUBLE
        # then to BIGINT to also tolerate any float-form ints.
        stripped = f"rtrim({value_sql}, 'i')"
        return f"cast(try_cast({stripped} AS DOUBLE) AS BIGINT)"
    if kind == "bool":
        # source emits 0/1 (float) for these; non-zero -> true, NULL stays NULL.
        return f"(try_cast({value_sql} AS DOUBLE) <> 0)"
    # string / fallback: strip LP's surrounding quotes if present
    return f"trim({value_sql}, '\"')"


def build_measurement_sql(
    measurement: str,
    schema: TableSchema,
    interval_ns: int,
    out_path: str,
    out_format: str,
) -> str:
    """Generate the DuckDB SQL that pivots one measurement and writes it out.

    The source view ``raw`` (created once by the caller) exposes columns
    ``measurement, head, field, val, ts``. Grouping is by ``head`` (the full
    ``measurement,tags`` string = series identity) and the bucket-end timestamp,
    so each tag becomes a constant-per-group column extracted from ``head``.
    """
    ts_col = schema.timestamp_col
    if interval_ns > 0:
        bucket_end = f"((ts // {interval_ns}) * {interval_ns} + {interval_ns})"
    else:
        bucket_end = "ts"

    selects: List[str] = [f"{bucket_end} AS \"{ts_col}\""]
    for col in schema.columns:
        if col.name == ts_col:
            continue
        if col.is_symbol:
            # Tag value from the head: ",name=" .. up to next comma. The leading
            # comma anchor avoids matching a field name; measurement has no '='.
            pat = f"(?:^|,){col.name}=([^,]*)"
            selects.append(
                f"any_value(regexp_extract(head, '{pat}', 1)) AS \"{col.name}\""
            )
        else:
            last_val = f"last(val ORDER BY ts) FILTER (WHERE field = '{col.name}')"
            selects.append(f"{_cast_expr(col.kind, last_val)} AS \"{col.name}\"")

    select_list = ",\n       ".join(selects)
    inner = (
        f"SELECT\n       {select_list}\n"
        f"FROM raw\nWHERE measurement = '{measurement}'\n"
        f"GROUP BY head, {bucket_end}"
    )
    fmt = "FORMAT PARQUET" if out_format == "parquet" else "FORMAT CSV, HEADER true"
    return f"COPY (\n{inner}\n) TO '{out_path}' ({fmt});"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", default="/dev/stdin",
                   help="sorted long-format LP file (default: stdin)")
    p.add_argument("--schema-file", required=True)
    p.add_argument("--prefix", default="batmon_tele_",
                   help="table-name prefix to strip when mapping table->measurement")
    p.add_argument("--downsample", default="20s", help="e.g. 20s, 1m (empty = exact)")
    p.add_argument("--out-dir", required=True, help="output directory")
    p.add_argument("--format", choices=("parquet", "csv"), default="parquet")
    p.add_argument("--memory-limit", default="1500MB",
                   help="DuckDB memory_limit (spills to --temp-dir beyond this)")
    p.add_argument("--temp-dir", default=None, help="DuckDB spill directory")
    p.add_argument("--threads", type=int, default=0, help="0 = DuckDB default (all cores)")
    p.add_argument("--measurement", action="append", default=[],
                   help="restrict to these measurements (repeatable); default: all in schema")
    p.add_argument("--print-sql", action="store_true", help="print generated SQL and exit")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    interval_ns = parse_interval_ns(args.downsample)

    with open(args.schema_file, encoding="utf-8") as fh:
        tables = parse_schema_tables(fh.read())
    if not tables:
        log.error("no CREATE TABLE found in %s", args.schema_file)
        return 2
    prefix = args.prefix
    by_meas: Dict[str, TableSchema] = {}
    for table, schema in tables.items():
        meas = table[len(prefix):] if prefix and table.startswith(prefix) else table
        by_meas[meas] = schema
    want = set(args.measurement) if args.measurement else set(by_meas)

    import os
    os.makedirs(args.out_dir, exist_ok=True)

    # The raw view parses each LP line once. read_csv with a single-space
    # delimiter yields exactly three columns for long-format input: the head
    # (no spaces), the single field=value token, and the epoch-ns timestamp.
    setup = [
        f"CREATE OR REPLACE VIEW raw AS SELECT "
        f"split_part(c0, ',', 1) AS measurement, "
        f"c0 AS head, "
        f"split_part(c1, '=', 1) AS field, "
        f"split_part(c1, '=', 2) AS val, "
        f"c2 AS ts "
        f"FROM read_csv('{args.input}', delim=' ', header=false, "
        f"columns={{'c0':'VARCHAR','c1':'VARCHAR','c2':'BIGINT'}}, "
        f"auto_detect=false, quote='', escape='');"
    ]

    stmts: List[str] = []
    for meas in sorted(want):
        schema = by_meas.get(meas)
        if schema is None:
            log.warning("no schema for measurement %s -- skipping", meas)
            continue
        out_path = os.path.join(args.out_dir, f"{meas}.{args.format}")
        stmts.append(build_measurement_sql(meas, schema, interval_ns, out_path, args.format))

    if args.print_sql:
        pragmas = [f"PRAGMA memory_limit='{args.memory_limit}';"]
        if args.threads > 0:
            pragmas.append(f"PRAGMA threads={args.threads};")
        if args.temp_dir:
            pragmas.append(f"PRAGMA temp_directory='{args.temp_dir}';")
        print("\n".join(pragmas))
        print("\n".join(setup))
        print("\n".join(stmts))
        return 0

    try:
        import duckdb
    except ImportError:
        log.error("duckdb module not installed -- run: pip install duckdb")
        return 3

    con = duckdb.connect()
    con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
    if args.threads > 0:
        con.execute(f"PRAGMA threads={args.threads}")
    if args.temp_dir:
        con.execute(f"PRAGMA temp_directory='{args.temp_dir}'")
    for s in setup:
        con.execute(s)
    t0 = time.time()
    total_rows = 0
    for meas, stmt in zip(sorted(want), stmts):
        con.execute(stmt)
        # COPY does not return a count; read it back cheaply from the file.
        out_path = stmt.split("TO '", 1)[1].split("'", 1)[0]
        n = con.execute(
            f"SELECT count(*) FROM read_{args.format}('{out_path}')"
        ).fetchone()[0]
        total_rows += n
        log.info("wrote %s rows -> %s", "{:,}".format(n), out_path)
    dt = time.time() - t0
    log.info("done: %s rows in %.1fs (%s rows/s)",
             "{:,}".format(total_rows), dt,
             "{:,}".format(int(total_rows / dt)) if dt else "inf")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
