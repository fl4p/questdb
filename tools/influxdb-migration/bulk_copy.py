#!/usr/bin/env python3
"""COPY-based bulk importer: export-lp -> wide CSV -> QuestDB parallel COPY.

This is the fastest supported path for a one-shot historical backfill from
InfluxDB. It exploits how BOTH databases store data:

* InfluxDB's ``influxd inspect export-lp`` / ``influx_inspect export`` streams one
  FIELD per line (its columnar storage shape), series-major.
* QuestDB's ``COPY`` (``ParallelCsvFileImporter``) is built for UNORDERED CSV: it
  sorts each partition by timestamp IN PARALLEL and writes column files directly,
  bypassing the ILP re-parse, the WAL sequencer, and out-of-order partition
  rewrites.

So the pipeline pivots the export into one wide CSV per measurement (via
:func:`pivot_lp.merge_stream` + :class:`csv_pivot.CsvSink`) and hands each file to
COPY. There is NO global external sort and NO WAL-apply throttle -- COPY does the
per-partition sort itself, and a pre-created table with ``DEDUP UPSERT KEYS`` makes
a resumed/re-run import idempotent.

COPY is single-flight (one import at a time), so measurements are loaded
serially: pivot the whole export to CSV files, then for each measurement
pre-create the table (or reuse an existing one), issue COPY, poll
``sys.text_import_log`` to completion, and delete the staged CSV.

The CSV files must live UNDER the server's ``cairo.sql.copy.root`` (COPY resolves
``FROM`` paths relative to it), so run this on the QuestDB host (or a box that
writes into the same directory). ``--copy-root`` is that local path;
``--copy-subdir`` is a staging subdirectory beneath it.

Source line protocol comes from one of:
* a built-in v1 export (``--database``/``--datadir``/``--waldir``), or
* ``--lp-file PATH`` (a pre-exported LP file), or
* ``--from-stdin`` (pipe ``influxd inspect export-lp`` for a v2 bucket).

Examples
--------
  # v2 bucket piped in, downsampled to 20s, schema from a CREATE TABLE file
  influxd inspect export-lp --bucket-id B --engine-path /data/engine --output-path - \\
    | bulk_copy.py --from-stdin --prefix batmon_tele_ --downsample 20s \\
        --schema-file tm-tables.sql --copy-root /var/lib/questdb/import \\
        --questdb-url http://localhost:9000 --user admin --password secret
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, Iterable, List, Optional

from bulk_v1 import build_export_cmd, parse_basic_or_token_auth, stream_export_lines
from csv_pivot import CsvSink
from pivot_lp import _make_on_progress, merge_stream, parse_interval_ns
from qdb_admin import (
    TableSchema,
    ensure_full_table,
    fetch_table_schema,
    parse_schema_tables,
)

log = logging.getLogger("influx_migrate.bulk_copy")

# QuestDB DateFormat pattern for a TIMESTAMP_NS designated timestamp parsed from
# the ISO-ns strings CsvSink writes (millis SSS + micros UUU + nanos NNN). For a
# microsecond TIMESTAMP column use 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' and ts-mode
# epoch/iso accordingly.
DEFAULT_TS_FORMAT = "yyyy-MM-ddTHH:mm:ss.SSSUUUNNNZ"

_ON_ERROR = {"abort": "ABORT", "skip_row": "SKIP_ROW", "skip_column": "SKIP_COLUMN"}


def build_copy_sql(
    table: str,
    rel_path: str,
    timestamp_col: str,
    timestamp_format: str,
    partition_by: str,
    on_error: str = "abort",
    delimiter: str = ",",
) -> str:
    """Build a ``COPY ... FROM ... WITH ...`` statement (options are space-sep).

    ``rel_path`` is the CSV path relative to ``cairo.sql.copy.root``. The table
    name is double-quoted so a measurement that is not a bare identifier still
    parses. ``on_error`` is one of abort/skip_row/skip_column. A non-comma
    ``delimiter`` adds an explicit ``DELIMITER`` option.
    """
    atom = _ON_ERROR.get(on_error.lower())
    if atom is None:
        raise ValueError(
            "on_error must be one of %s, got %r" % (", ".join(_ON_ERROR), on_error)
        )
    sql = (
        "COPY \"%s\" FROM '%s' WITH HEADER true "
        "TIMESTAMP '%s' FORMAT '%s' PARTITION BY %s ON ERROR %s"
        % (table, rel_path, timestamp_col, timestamp_format, partition_by, atom)
    )
    if delimiter != ",":
        sql += " DELIMITER '%s'" % delimiter
    return sql


def _exec_query(base_url: str, auth: Optional[str], sql: str, timeout: float = 60.0):
    """POST ``sql`` to ``/exec`` and return the parsed JSON doc (raise on error)."""
    url = base_url.rstrip("/") + "/exec?" + urllib.parse.urlencode({"query": sql})
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", auth)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace").strip()
        raise SystemExit(
            "QuestDB /exec failed (HTTP %d) for: %s\n%s" % (exc.code, sql, detail)
        ) from exc
    except urllib.error.URLError as exc:
        raise SystemExit("QuestDB /exec unreachable: %s" % exc.reason) from exc


def run_copy(base_url: str, auth: Optional[str], copy_sql: str) -> str:
    """Submit a COPY and return its hex import id (COPY returns one row, one col)."""
    doc = _exec_query(base_url, auth, copy_sql)
    dataset = doc.get("dataset") or []
    if not dataset or not dataset[0]:
        raise SystemExit("COPY did not return an import id; response: %s" % doc)
    return str(dataset[0][0])


def poll_copy(
    base_url: str,
    auth: Optional[str],
    import_id: str,
    sleep_fn,
    poll_secs: float = 2.0,
) -> dict:
    """Block until the COPY identified by ``import_id`` finishes; return its row.

    Polls the import-level rows of ``sys.text_import_log`` (``phase IS NULL`` --
    the ``started`` row and the terminal ``finished``/``failed``/``cancelled``
    row). Raises SystemExit on a failed/cancelled import (surfacing ``message``).
    ``sleep_fn`` is the sleep seam (``time.sleep`` in production, a recorder in
    tests).
    """
    sql = (
        "SELECT status, rows_imported, errors, message "
        "FROM sys.text_import_log WHERE id = '%s' AND phase IS NULL "
        "ORDER BY ts DESC LIMIT 1" % import_id
    )
    while True:
        doc = _exec_query(base_url, auth, sql)
        dataset = doc.get("dataset") or []
        if dataset:
            row = dataset[0]
            status = row[0]
            if status == "finished":
                log.info(
                    "copy %s: finished, %s rows imported (%s errors)",
                    import_id,
                    "{:,}".format(row[1] or 0),
                    row[2] or 0,
                )
                return {"status": status, "rows_imported": row[1], "errors": row[2]}
            if status in ("failed", "cancelled"):
                raise SystemExit(
                    "COPY %s %s: %s" % (import_id, status, row[3] or "(no message)")
                )
        sleep_fn(poll_secs)


def _resolve_schemas(
    args: argparse.Namespace, prefix: str, auth: Optional[str]
):
    """Build (file_schemas, resolve) -- schema file first, existing table second.

    Returns the file-derived ``{measurement: TableSchema}`` (for pre-create) and a
    ``resolve(measurement)`` the CsvSink calls lazily: a measurement absent from
    the file falls back to an existing table's live schema unless
    ``--no-schema-from-table`` is set.
    """
    file_schemas: Dict[str, TableSchema] = {}
    if args.schema_file:
        with open(args.schema_file, encoding="utf-8") as fh:
            tables = parse_schema_tables(fh.read())
        if not tables:
            raise SystemExit("no CREATE TABLE statements found in %s" % args.schema_file)
        for table, schema in tables.items():
            meas = table[len(prefix):] if prefix and table.startswith(prefix) else table
            file_schemas[meas] = schema
        log.info(
            "schema for %d measurement(s) from %s: %s",
            len(file_schemas),
            args.schema_file,
            sorted(file_schemas),
        )

    def resolve(measurement: str) -> Optional[TableSchema]:
        schema = file_schemas.get(measurement)
        if schema is not None:
            return schema
        if args.schema_from_table:
            return fetch_table_schema(args.questdb_url, auth, prefix + measurement)
        return None

    return file_schemas, resolve


def _source_lines(args: argparse.Namespace) -> Iterable[str]:
    """Yield source line-protocol lines from the chosen input."""
    if args.from_stdin:
        log.info("reading line protocol from stdin")
        for raw in sys.stdin:
            line = raw.rstrip("\n")
            if line and not line.startswith("#"):
                yield line
        return
    if args.lp_file:
        log.info("reading line protocol from %s", args.lp_file)
        with open(args.lp_file, encoding="utf-8") as fh:
            for raw in fh:
                line = raw.rstrip("\n")
                if line and not line.startswith("#"):
                    yield line
        return
    cmd = build_export_cmd(
        args.database,
        args.datadir,
        args.waldir,
        retention=args.retention,
        start=args.start,
        end=args.end,
        binary=args.binary,
    )
    yield from stream_export_lines(cmd)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    prefix = "" if args.no_prefix else args.prefix
    interval_ns = parse_interval_ns(args.downsample)
    if interval_ns:
        log.info("downsampling to a fixed %s grid (last value per field per bucket)", args.downsample)
        if not args.assume_sorted:
            log.warning(
                "downsample requires TIMESTAMP-SORTED input. The raw export-lp is "
                "series-major, so sort it first (timestamp-major, e.g. the "
                "awk-prepend sort in import_batmon_copy.sh) and pass "
                "--assume-sorted. The pivot aborts if it sees unsorted input."
            )

    if not args.from_stdin and not args.lp_file and not args.database:
        log.error("a source is required: --from-stdin, --lp-file, or --database (+ --datadir/--waldir)")
        return 2

    auth = parse_basic_or_token_auth(args.user, args.password, args.token)
    file_schemas, resolve = _resolve_schemas(args, prefix, auth)

    staging_dir = os.path.join(args.copy_root, args.copy_subdir)
    os.makedirs(staging_dir, exist_ok=True)

    # 1. Pivot the whole export into one wide CSV per measurement (unordered;
    #    COPY sorts). One streaming pass, bounded memory.
    sink = CsvSink(
        staging_dir,
        resolve,
        prefix=prefix,
        delimiter=args.csv_delimiter,
        timestamp_mode=args.csv_timestamp_mode,
    )
    points, field_lines = merge_stream(
        _source_lines(args), prefix, None, interval_ns, None, _make_on_progress(), sink
    )
    written = sink.paths  # {measurement: abs_path under staging_dir}
    log.info(
        "pivot: wrote %s %s points to %d CSV file(s) from %s field-lines",
        "{:,}".format(points),
        "downsampled" if interval_ns else "wide",
        len(written),
        "{:,}".format(field_lines),
    )
    if not written:
        log.warning("no CSV files written (no resolvable measurements); nothing to COPY")
        return 0

    # 2. COPY each measurement serially (COPY is single-flight).
    total_imported = 0
    for measurement in sorted(written):
        table = prefix + measurement
        csv_path = written[measurement]
        rel_path = os.path.join(args.copy_subdir, table + ".csv")
        copy_sql = build_copy_sql(
            table,
            rel_path,
            args.copy_timestamp_col,
            args.copy_timestamp_format,
            args.partition_by,
            on_error=args.on_error,
            delimiter=args.csv_delimiter,
        )

        # Dry-run mutates nothing: no pre-create, no COPY -- just show the plan.
        if args.dry_run:
            log.info("[dry-run] %s", copy_sql)
            continue

        if args.create_tables:
            schema = file_schemas.get(measurement)
            if schema is not None:
                ensure_full_table(
                    args.questdb_url,
                    auth,
                    schema,
                    timestamp_type=args.timestamp_type or None,
                    partition_by=args.partition_by,
                    dedup=not args.no_dedup,
                )
            else:
                log.info(
                    "table %s: not in --schema-file; assuming it already exists "
                    "(schema read from QuestDB)",
                    table,
                )

        log.info("COPY %s FROM %s ...", table, rel_path)
        import_id = run_copy(args.questdb_url, auth, copy_sql)
        result = poll_copy(args.questdb_url, auth, import_id, _sleep, args.poll_secs)
        total_imported += int(result.get("rows_imported") or 0)

        if not args.keep_csv:
            try:
                os.remove(csv_path)
            except OSError as exc:
                log.warning("could not delete staged CSV %s: %s", csv_path, exc)

    if not args.dry_run:
        log.info(
            "done: imported %s rows across %d table(s)",
            "{:,}".format(total_imported),
            len(written),
        )
    return 0


def _sleep(secs: float) -> None:
    import time

    time.sleep(secs)


def _parse_args(argv: Optional[List[str]]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="bulk_copy.py",
        description="Pivot InfluxDB export-lp into wide CSV and bulk-load via "
        "QuestDB COPY (parallel, no global sort, no WAL/O3).",
    )
    # source
    p.add_argument("--from-stdin", action="store_true", help="read LP from stdin")
    p.add_argument("--lp-file", default="", help="read LP from this file")
    p.add_argument("--database", default="", help="v1 source database (built-in export)")
    p.add_argument("--datadir", default="", help="v1 data dir")
    p.add_argument("--waldir", default="", help="v1 wal dir")
    p.add_argument("--retention", default="", help="v1 retention policy")
    p.add_argument("--start", default="", help="RFC3339 export start (inclusive)")
    p.add_argument("--end", default="", help="RFC3339 export end (exclusive)")
    p.add_argument("--binary", default="influx_inspect", help="v1 export binary")
    # pivot
    p.add_argument("--prefix", default="", help="measurement prefix, e.g. 'batmon_tele_'")
    p.add_argument("--no-prefix", action="store_true")
    p.add_argument(
        "--downsample",
        default="0",
        help="downsample onto a fixed grid (Ns/Nm/Nh/Nd), last value per field "
        "per bucket; 0/none = exact pivot.",
    )
    p.add_argument(
        "--schema-file",
        default="",
        help="CREATE TABLE schema (authoritative column set/types/partitioning). "
        "A measurement absent here falls back to an existing table's schema.",
    )
    p.add_argument(
        "--no-schema-from-table",
        dest="schema_from_table",
        action="store_false",
        help="do NOT read an existing table's schema for measurements absent from "
        "--schema-file (default: do).",
    )
    p.set_defaults(schema_from_table=True)
    # csv / copy
    p.add_argument(
        "--copy-root",
        required=True,
        help="local path to the server's cairo.sql.copy.root (CSVs are staged "
        "under it; COPY resolves FROM paths relative to it).",
    )
    p.add_argument(
        "--copy-subdir",
        default="influx-import",
        help="staging subdirectory under --copy-root (default 'influx-import').",
    )
    p.add_argument("--csv-delimiter", default=",", help="CSV delimiter (default ',').")
    p.add_argument(
        "--csv-timestamp-mode",
        default="iso-ns",
        choices=("iso-ns", "epoch-ns"),
        help="CSV timestamp encoding (default iso-ns; pair with --copy-timestamp-format).",
    )
    p.add_argument(
        "--copy-timestamp-col",
        default="timestamp",
        help="designated timestamp column name for COPY (default 'timestamp').",
    )
    p.add_argument(
        "--copy-timestamp-format",
        default=DEFAULT_TS_FORMAT,
        help="COPY timestamp FORMAT. Default %s (ISO-ns into a TIMESTAMP_NS "
        "column). For micros use 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ'." % DEFAULT_TS_FORMAT,
    )
    p.add_argument("--partition-by", default="DAY", help="PARTITION BY unit (default DAY).")
    p.add_argument(
        "--on-error",
        default="abort",
        choices=("abort", "skip_row", "skip_column"),
        help="COPY ON ERROR behaviour (default abort).",
    )
    p.add_argument(
        "--timestamp-type",
        default="TIMESTAMP_NS",
        help="designated timestamp type used when pre-creating tables (default "
        "TIMESTAMP_NS). Empty = use the schema file's declared type.",
    )
    p.add_argument(
        "--no-create-tables",
        dest="create_tables",
        action="store_false",
        help="do NOT pre-create tables (they must already exist).",
    )
    p.set_defaults(create_tables=True)
    p.add_argument(
        "--no-dedup",
        action="store_true",
        help="omit DEDUP UPSERT KEYS when pre-creating (default: add it for "
        "idempotent resumes).",
    )
    p.add_argument(
        "--assume-sorted",
        action="store_true",
        help="acknowledge the input is already timestamp-sorted (downsample "
        "requires it). Without this, a reminder is logged; the pivot aborts "
        "loudly either way if it detects unsorted input.",
    )
    p.add_argument("--keep-csv", action="store_true", help="keep staged CSVs after COPY")
    p.add_argument("--poll-secs", type=float, default=2.0, help="COPY status poll interval")
    # connection
    p.add_argument("--questdb-url", default="http://localhost:9000")
    p.add_argument("--user", default="")
    p.add_argument("--password", default="")
    p.add_argument("--token", default="")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="write CSVs and print the COPY statements, but do not execute COPY.",
    )
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
