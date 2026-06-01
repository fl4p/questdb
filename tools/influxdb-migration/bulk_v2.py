#!/usr/bin/env python3
"""Fast bulk import of an InfluxDB v2 bucket into QuestDB via direct TSM export.

The Flux query path does not scale to very high series cardinality: a bucket
with tens of thousands of series makes server-side ``pivot()`` time out, because
every query re-enumerates all series. This module bypasses the query engine
entirely. ``influxd inspect export-lp`` reads the bucket's TSM (and WAL) files
straight off disk and emits InfluxDB line protocol, which is exactly what
QuestDB's ILP-over-HTTP ``/write`` endpoint ingests. We rewrite each line's
measurement to the ``<bucket>_<measurement>`` naming contract and POST it in
batches.

The reusable line-protocol rewrite (escaping-aware) and the ILP-HTTP feeder live
in :mod:`bulk_v1`; this module only builds the v2 export command and drives the
stream with a progress heartbeat.

The engine files are owned by the InfluxDB service user, so the export usually
needs privilege: pass ``--sudo`` to run ``influxd inspect`` via ``sudo -n`` (set
up NOPASSWD for ``influxd inspect`` first). ACL/principals/manifest generation
stays with the main migration tool; this moves bulk data only.
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Dict, List, Optional

from bulk_v1 import (
    _IlpHttpFeeder,
    measurement_of,
    parse_basic_or_token_auth,
    rewrite_line,
    stream_export_lines,
)
from model import scope_prefix, validate_scope

log = logging.getLogger("influx_migrate.bulk_v2")


def build_export_cmd(
    bucket_id: str,
    engine_path: str,
    measurement: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    use_sudo: bool = False,
    binary: str = "influxd",
) -> List[str]:
    """Build the ``influxd inspect export-lp`` command line.

    ``--output-path -`` streams line protocol to stdout. ``export-lp`` DOES
    support ``--measurement``, ``--start`` and ``--end``, so unlike the v1 tool
    we push those down to the source. With ``use_sudo`` the command runs under
    ``sudo -n`` (non-interactive: requires a NOPASSWD rule for ``influxd
    inspect`` so a missing password fails fast rather than hanging on a prompt).
    """
    cmd: List[str] = ["sudo", "-n"] if use_sudo else []
    cmd += [
        binary,
        "inspect",
        "export-lp",
        "--bucket-id",
        bucket_id,
        "--engine-path",
        engine_path,
        "--output-path",
        "-",
    ]
    if measurement:
        cmd += ["--measurement", measurement]
    if start:
        cmd += ["--start", start]
    if end:
        cmd += ["--end", end]
    return cmd


def run_bulk_v2(
    bucket_name: str,
    cmd: List[str],
    prefix: str,
    feeder: Optional["_IlpHttpFeeder"],
    expect_lines: int = 0,
    heartbeat_every: int = 50_000,
) -> Dict[str, int]:
    """Stream the export, rewrite each line, feed QuestDB, log a heartbeat.

    Returns a per-target-table line count. With ``feeder`` None this is a dry
    run: lines are rewritten and counted but nothing is POSTed. The heartbeat
    logs cumulative lines, rate and (when ``expect_lines`` is set) a percentage,
    so a multi-million-line export visibly progresses instead of going silent.
    """
    counts: Dict[str, int] = {}
    total = 0
    t0 = time.monotonic()
    last = 0.0
    for line in stream_export_lines(cmd):
        if not line:
            continue
        src = measurement_of(line)
        target = prefix + src
        counts[target] = counts.get(target, 0) + 1
        if feeder is not None:
            feeder.add(rewrite_line(line, prefix))
        total += 1
        if total % heartbeat_every == 0:
            now = time.monotonic()
            if now - last >= 2.0:
                last = now
                elapsed = now - t0
                rate = total / elapsed if elapsed > 0 else 0.0
                pct = ""
                if expect_lines > 0:
                    pct = " (~%.0f%% of %s)" % (
                        100.0 * total / expect_lines,
                        "{:,}".format(expect_lines),
                    )
                verb = "read" if feeder is None else "wrote"
                log.info(
                    "progress: %s %s lines | %s lines/s | %d tables%s",
                    verb,
                    "{:,}".format(total),
                    "{:,}".format(int(rate)),
                    len(counts),
                    pct,
                )
    if feeder is not None:
        feeder.flush()
    return counts


def _report(counts: Dict[str, int], dry_run: bool) -> None:
    verb = "[dry-run] would import" if dry_run else "imported"
    total = 0
    for table, n in sorted(counts.items()):
        total += n
        log.info("%s %d lines -> %s", verb, n, table)
    if not counts:
        log.info("%s 0 lines (export produced no points)", verb)
    log.info("%s %d lines across %d table(s)", verb, total, len(counts))


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    if args.no_prefix:
        prefix = ""
    else:
        validate_scope(args.bucket_name)
        prefix = scope_prefix(args.bucket_name)

    cmd = build_export_cmd(
        bucket_id=args.bucket_id,
        engine_path=args.engine_path,
        measurement=args.measurement,
        start=args.start,
        end=args.end,
        use_sudo=args.sudo,
        binary=args.influxd_binary,
    )
    log.info("export command: %s", " ".join(cmd))

    feeder: Optional[_IlpHttpFeeder] = None
    if not args.dry_run:
        auth = parse_basic_or_token_auth(args.user, args.password, args.token)
        feeder = _IlpHttpFeeder(args.questdb_url, args.batch_size, auth)

    counts = run_bulk_v2(
        args.bucket_name, cmd, prefix, feeder, expect_lines=args.expect_rows
    )
    _report(counts, args.dry_run)
    return 0


def _parse_args(argv: Optional[List[str]]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="bulk_v2.py",
        description="Bulk-import an InfluxDB v2 bucket into QuestDB via "
        "influxd inspect export-lp (bypasses the Flux query engine).",
    )
    src = p.add_argument_group("source (InfluxDB v2 engine)")
    src.add_argument("--bucket-id", required=True, help="bucket ID to export")
    src.add_argument(
        "--bucket-name",
        required=True,
        help="bucket name; drives the <bucket>_ table prefix (naming contract)",
    )
    src.add_argument(
        "--engine-path",
        required=True,
        help="influxd engine-path (see /etc/influxdb/config.toml engine-path)",
    )
    src.add_argument("--measurement", help="export only this measurement")
    src.add_argument("--start", help="optional export start (RFC3339)")
    src.add_argument("--end", help="optional export end (RFC3339)")
    src.add_argument(
        "--sudo",
        action="store_true",
        help="run influxd inspect under 'sudo -n' (engine files are owned by "
        "the influxdb service user; needs a NOPASSWD rule for influxd inspect)",
    )
    src.add_argument("--influxd-binary", default="influxd", help="influxd binary")

    tgt = p.add_argument_group("target (QuestDB)")
    tgt.add_argument("--questdb-url", default="http://localhost:9000")
    tgt.add_argument("--batch-size", type=int, default=10_000)
    tgt.add_argument("--user", default="", help="QuestDB HTTP basic-auth user")
    tgt.add_argument("--password", default="", help="QuestDB HTTP basic-auth password")
    tgt.add_argument("--token", default="", help="QuestDB HTTP bearer token")

    p.add_argument("--no-prefix", action="store_true", help="single-bucket: no prefix")
    p.add_argument(
        "--expect-rows",
        type=int,
        default=0,
        help="approximate total line count, for the progress percentage",
    )
    p.add_argument("--dry-run", action="store_true", help="stream + count, POST nothing")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
