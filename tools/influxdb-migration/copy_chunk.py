#!/usr/bin/env python3
"""Serial COPY consumer for the parallel backfill driver (import_batmon_parallel.sh).

Takes a directory of per-measurement wide CSVs written by ``pivot_lp --csv-out-dir``
for ONE time chunk (staged under the server's ``cairo.sql.copy.root``) and loads
each into its QuestDB table with ``COPY`` -- single-flight, polled to completion,
deleted after. ``COPY`` is O3-free and order-tolerant (``ParallelCsvFileImporter``
sorts each partition itself), so chunks may be PRODUCED in parallel and drained
here serially in any order, with no out-of-order partition rewrites and idempotent
re-runs via the tables' ``DEDUP UPSERT KEYS``.

It reuses ``bulk_copy``'s COPY primitives verbatim, so the COPY statement, the
timestamp FORMAT, and table pre-creation stay identical to the single-shot
``bulk_copy.py`` path -- this consumer only adds the "load an already-pivoted
chunk directory" entry point that the parallel driver needs.
"""

from __future__ import annotations

import argparse
import glob
import logging
import os

from bulk_copy import DEFAULT_TS_FORMAT, build_copy_sql, poll_copy, run_copy, _sleep
from bulk_v1 import parse_basic_or_token_auth
from qdb_admin import ensure_full_table, parse_schema_tables

log = logging.getLogger("copy_chunk")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--csv-dir", required=True,
                   help="directory of <table>.csv for ONE chunk (absolute path, under copy root)")
    p.add_argument("--copy-subdir", required=True,
                   help="that directory's path RELATIVE to the server's copy root "
                        "(COPY resolves FROM paths relative to cairo.sql.copy.root)")
    p.add_argument("--schema-file", default=None,
                   help="CREATE TABLE source; required only with --create-tables")
    p.add_argument("--prefix", default="batmon_tele_")
    p.add_argument("--questdb-url", default="http://localhost:9000")
    p.add_argument("--user", default=None)
    p.add_argument("--password", default=None)
    p.add_argument("--token", default=None)
    p.add_argument("--copy-timestamp-col", default="timestamp")
    p.add_argument("--copy-timestamp-format", default=DEFAULT_TS_FORMAT)
    p.add_argument("--partition-by", default="DAY")
    p.add_argument("--on-error", default="abort")
    p.add_argument("--csv-delimiter", default=",")
    p.add_argument("--create-tables", action="store_true",
                   help="ensure each table exists (full schema, DEDUP) before COPY; needs --schema-file")
    p.add_argument("--timestamp-type", default=None)
    p.add_argument("--no-dedup", action="store_true")
    p.add_argument("--keep-csv", action="store_true", help="do not delete a CSV after its COPY")
    p.add_argument("--poll-secs", type=float, default=2.0)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    auth = parse_basic_or_token_auth(args.user, args.password, args.token)

    schemas = {}
    if args.schema_file:
        with open(args.schema_file, encoding="utf-8") as fh:
            for table, sc in parse_schema_tables(fh.read()).items():
                meas = table[len(args.prefix):] if args.prefix and table.startswith(args.prefix) else table
                schemas[meas] = sc

    csvs = sorted(glob.glob(os.path.join(args.csv_dir, "*.csv")))
    if not csvs:
        log.info("no CSVs in %s -- nothing to COPY", args.csv_dir)
        return 0

    total = 0
    for path in csvs:
        fname = os.path.basename(path)          # <prefix><measurement>.csv
        table = fname[:-4]                       # strip ".csv"
        meas = table[len(args.prefix):] if args.prefix and table.startswith(args.prefix) else table
        rel = os.path.join(args.copy_subdir, fname)

        if args.create_tables:
            schema = schemas.get(meas)
            if schema is not None:
                ensure_full_table(
                    args.questdb_url, auth, schema,
                    timestamp_type=args.timestamp_type or None,
                    partition_by=args.partition_by, dedup=not args.no_dedup,
                )
            else:
                log.info("table %s not in --schema-file; assuming it already exists", table)

        copy_sql = build_copy_sql(
            table, rel, args.copy_timestamp_col, args.copy_timestamp_format,
            args.partition_by, on_error=args.on_error, delimiter=args.csv_delimiter,
        )
        log.info("COPY %s FROM %s ...", table, rel)
        import_id = run_copy(args.questdb_url, auth, copy_sql)
        result = poll_copy(args.questdb_url, auth, import_id, _sleep, args.poll_secs)
        total += int(result.get("rows_imported") or 0)

        if not args.keep_csv:
            try:
                os.remove(path)
            except OSError as exc:
                log.warning("could not delete staged CSV %s: %s", path, exc)

    log.info("chunk COPY done: %s rows from %d file(s)", "{:,}".format(total), len(csvs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
