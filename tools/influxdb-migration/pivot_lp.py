#!/usr/bin/env python3
"""Combine InfluxDB line protocol into wide points, then feed QuestDB.

``influxd inspect export-lp`` emits one line per FIELD (InfluxDB's columnar
storage shape): ``batmon,device=X voltage_cell000=3327i <ts>`` on one line,
``batmon,device=X current=-0.5 <ts>`` on another -- even when both belong to the
same point. QuestDB's ``DEDUP`` keeps one row per key WITHOUT merging columns, so
it cannot reassemble points. This tool does the pivot explicitly: it merges all
field lines that share the same ``(measurement + tags, timestamp)`` key into a
single wide line protocol line, then POSTs to QuestDB's ``/write`` endpoint.

It reads from stdin and assumes the input is SORTED so that lines sharing the
``(measurement+tags, timestamp)`` key are adjacent. For the exact pivot either
``sort -k1,1 -k3,3n`` (series-first) or ``sort -k3,3n -k1,1`` (timestamp-first)
works. For ``--downsample`` the input MUST be timestamp-first (``sort -k3,3n
-k1,1``): that makes the merged output timestamp-ordered too, so QuestDB appends
it instead of doing slow out-of-order partition rewrites. Either way memory is
bounded -- only the current point (exact) or the current interval's series
(downsample) is held, never the whole stream. The measurement is rewritten to
``<prefix><measurement>`` via the shared, escaping-aware rewrite in
:mod:`bulk_v1`.

Input line shape (one field): ``<measurement,tags> <field=value> <timestamp>``.
Output line shape (merged):  ``<prefix+measurement,tags> <f1=v1,f2=v2,...> <ts>``.

Tag values and field values must not contain spaces (true for the numeric
battery/telemetry data this targets); a measurement/tag with spaces would need a
full escaping-aware splitter (a documented TODO).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional

from bulk_v1 import _IlpHttpFeeder, parse_basic_or_token_auth, rewrite_line
from qdb_admin import IndexSpecError, ensure_indexed_table, parse_index_spec

log = logging.getLogger("influx_migrate.pivot")


class _WalThrottle:
    """Pauses feeding when QuestDB's WAL apply backlog grows too large.

    ILP-over-HTTP accepts batches into the WAL sequencer far faster than the
    table writer applies them, so under a sustained bulk feed the un-applied
    backlog (``sequencerTxn - writerTxn``, one txn per ~``batch_size``-row POST)
    can balloon to tens of millions of rows -- memory pressure with no upper
    bound. This throttle queries ``wal_tables()`` and, once the estimated pending
    rows cross ``high``, blocks the feed (which back-pressures the upstream
    ``sort | pivot`` pipe) until the backlog drains below ``low``. The lag metric
    is dedup-safe: it reflects real writer-vs-sequencer apply lag, not a
    sent-minus-counted row delta that DEDUP would distort.

    The pending-row figure is an ESTIMATE: ``pending_txns * batch_size``. Each
    /write POST of up to ``batch_size`` lines lands as roughly one WAL commit, so
    the product tracks the true backlog closely but is not exact (the trailing
    batch and any server-side splitting vary the rows-per-txn).
    """

    def __init__(
        self,
        base_url: str,
        auth: Optional[str],
        prefix: str,
        batch_size: int,
        high_rows: int,
        low_rows: int,
        poll_secs: float,
    ):
        self._exec = base_url.rstrip("/") + "/exec"
        self._auth = auth
        self._prefix = prefix
        self._batch_size = max(1, batch_size)
        self._high = high_rows
        self._low = low_rows if low_rows > 0 else max(1, high_rows // 2)
        self._poll = max(0.5, poll_secs)

    def _pending_rows(self) -> Optional[int]:
        """Estimate pending (un-applied) rows across the target tables.

        Returns None when the query fails -- the caller treats that as "can't
        measure, don't block" so a transient HTTP hiccup never hangs the import.
        """
        sql = "SELECT name, writerTxn, sequencerTxn FROM wal_tables()"
        url = self._exec + "?" + urllib.parse.urlencode({"query": sql})
        req = urllib.request.Request(url)
        if self._auth:
            req.add_header("Authorization", self._auth)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                doc = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, ValueError) as exc:
            log.warning("throttle: wal_tables() query failed (%s); not throttling", exc)
            return None
        pending_txns = 0
        for row in doc.get("dataset", []):
            name = row[0] or ""
            if self._prefix and not name.startswith(self._prefix):
                continue
            writer_txn = row[1] or 0
            sequencer_txn = row[2] or 0
            lag = sequencer_txn - writer_txn
            if lag > 0:
                pending_txns += lag
        return pending_txns * self._batch_size

    def wait_if_needed(self) -> None:
        """Block until the backlog drains below ``low`` if it exceeds ``high``."""
        pending = self._pending_rows()
        if pending is None or pending < self._high:
            return
        log.info(
            "throttle: ~%s pending rows >= %s; pausing feed until <= %s",
            "{:,}".format(pending),
            "{:,}".format(self._high),
            "{:,}".format(self._low),
        )
        waited = 0.0
        while True:
            time.sleep(self._poll)
            waited += self._poll
            pending = self._pending_rows()
            if pending is None:
                log.warning(
                    "throttle: lost pending signal after %.0fs; resuming feed", waited
                )
                return
            if pending <= self._low:
                log.info(
                    "throttle: ~%s pending rows <= %s; resuming feed after %.0fs",
                    "{:,}".format(pending),
                    "{:,}".format(self._low),
                    waited,
                )
                return
            if waited % 30 < self._poll:
                log.info(
                    "throttle: still waiting, ~%s pending rows (%.0fs elapsed)",
                    "{:,}".format(pending),
                    waited,
                )


class _ThrottledFeeder(_IlpHttpFeeder):
    """``_IlpHttpFeeder`` that consults a :class:`_WalThrottle` after flushes.

    It checks the backlog only every ``check_every`` batches (not every batch) to
    keep the extra ``wal_tables()`` queries negligible, then delegates the actual
    pause decision to the throttle. Pausing here naturally back-pressures the
    upstream ``sort | pivot`` pipe, so the source stops being read until QuestDB
    catches up.
    """

    def __init__(
        self,
        base_url: str,
        batch_size: int,
        auth: Optional[str],
        throttle: "_WalThrottle",
        check_every: int = 20,
        timeout: float = 60.0,
    ):
        super().__init__(base_url, batch_size, auth, timeout)
        self._throttle = throttle
        self._check_every = max(1, check_every)
        self._batches_since_check = 0

    def flush(self) -> None:
        before = self.batches_sent
        super().flush()
        if self.batches_sent == before:
            return
        self._batches_since_check += 1
        if self._batches_since_check >= self._check_every:
            self._batches_since_check = 0
            self._throttle.wait_if_needed()


def _split_lp(line: str):
    """Split a line-protocol line into (head, fields, timestamp).

    head = ``measurement,tags`` (up to the first space); timestamp = after the
    last space; fields = everything between. Uses first/last space, so commas in
    the field set are preserved. Returns None for a line without two spaces.
    """
    i1 = line.find(" ")
    if i1 < 0:
        return None
    i2 = line.rfind(" ")
    if i2 <= i1:
        return None
    return line[:i1], line[i1 + 1 : i2], line[i2 + 1 :]


def parse_interval_ns(spec: str) -> int:
    """Parse a downsample interval into nanoseconds. 0 / '' / 'none' = off.

    Accepts a bare nanosecond integer or a number with an s/m/h/d suffix
    (seconds/minutes/hours/days), e.g. '10s', '1m', '5m', '1h', '1d'.
    """
    s = (spec or "").strip().lower()
    if not s or s in ("0", "none", "off"):
        return 0
    mult = {
        "s": 1_000_000_000,
        "m": 60_000_000_000,
        "h": 3_600_000_000_000,
        "d": 86_400_000_000_000,
    }
    if s[-1] in mult:
        return int(float(s[:-1]) * mult[s[-1]])
    return int(s)


def merge_stream(
    lines, prefix: str, feeder: Optional["_IlpHttpFeeder"], interval_ns: int = 0
):
    """Merge single-field LP lines into wide points; feed or count.

    Returns (points, field_lines).

    EXACT mode (``interval_ns == 0``) groups by the exact ``(head, timestamp)``
    -- a faithful pivot. It only needs each point's field lines to be ADJACENT,
    which both a head-first and a timestamp-first sort guarantee.

    DOWNSAMPLE mode (``interval_ns > 0``) snaps onto one fixed time grid applied
    uniformly to every field/column: half-open buckets ``[k*interval,
    (k+1)*interval)``, keeping the LAST value seen for each field name in the
    bucket and stamping the row at the bucket END ``(k+1)*interval``.

    DOWNSAMPLE REQUIRES TIMESTAMP-SORTED INPUT (``sort -k3,3n ...``). Because the
    timestamps are non-decreasing, exactly ONE bucket is open at a time across
    ALL series: when a line's timestamp crosses into a later bucket, every
    series' accumulator for the prior bucket is complete and flushed at once.
    That makes the OUTPUT timestamp-ordered too, so QuestDB appends it instead
    of doing expensive out-of-order partition rewrites. Memory is bounded by the
    number of distinct series active within a single interval, not the stream
    length.

    NO LOOK-AHEAD: the row is RIGHT-labeled. The kept value is the last sample in
    ``[start, end)``, which occurred strictly before ``end``, so every row's
    value lies strictly in that row timestamp's past -- a reader at time ``end``
    could legitimately have known it. Labeling at the bucket START instead would
    attach a value measured up to one interval in the FUTURE -> look-ahead bias.
    """
    points = 0
    field_lines = 0

    if interval_ns:
        cur_bucket: Optional[int] = None
        acc: Dict[str, Dict[str, str]] = {}  # head -> {field_name: "name=value"}

        def flush_bucket():
            nonlocal points
            if cur_bucket is None:
                return
            ts_label = str(cur_bucket + interval_ns)
            for head, fmap in acc.items():
                out = rewrite_line(
                    head + " " + ",".join(fmap.values()) + " " + ts_label, prefix
                )
                if feeder is not None:
                    feeder.add(out)
                points += 1
            acc.clear()

        for raw in lines:
            raw = raw.rstrip("\n")
            if not raw:
                continue
            parts = _split_lp(raw)
            if parts is None:
                continue
            head, fields, ts = parts
            try:
                bucket = (int(ts) // interval_ns) * interval_ns
            except ValueError:
                continue
            field_lines += 1
            if cur_bucket is None:
                cur_bucket = bucket
            elif bucket != cur_bucket:
                # ts is non-decreasing, so the prior bucket is now complete.
                flush_bucket()
                cur_bucket = bucket
            fmap = acc.get(head)
            if fmap is None:
                fmap = {}
                acc[head] = fmap
            for tok in fields.split(","):
                fmap[tok.split("=", 1)[0]] = tok
        flush_bucket()
        if feeder is not None:
            feeder.flush()
        return points, field_lines

    cur_key = None
    cur_head: Optional[str] = None
    cur_ts: Optional[str] = None
    cur_fields: List[str] = []

    def flush_point():
        nonlocal points
        if cur_head is None or cur_ts is None:
            return
        out = rewrite_line(
            cur_head + " " + ",".join(cur_fields) + " " + cur_ts, prefix
        )
        if feeder is not None:
            feeder.add(out)
        points += 1

    for raw in lines:
        raw = raw.rstrip("\n")
        if not raw:
            continue
        parts = _split_lp(raw)
        if parts is None:
            continue
        head, fields, ts = parts
        field_lines += 1
        key = (head, ts)
        if key != cur_key:
            flush_point()
            cur_head, cur_key, cur_ts, cur_fields = head, key, ts, [fields]
        else:
            cur_fields.append(fields)
    flush_point()
    if feeder is not None:
        feeder.flush()
    return points, field_lines


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
    if args.index and not args.create_table:
        log.error("--index requires --create-table NAME (the table to pre-create)")
        return 2

    feeder: Optional[_IlpHttpFeeder] = None
    if not args.dry_run:
        auth = parse_basic_or_token_auth(args.user, args.password, args.token)
        # Pre-create the target table BEFORE feeding so requested SYMBOL indexes
        # exist (ILP auto-create never adds indexes). Other columns auto-create.
        if args.create_table:
            try:
                specs = parse_index_spec(args.index)
            except IndexSpecError as exc:
                log.error("bad --index spec: %s", exc)
                return 2
            ensure_indexed_table(
                args.questdb_url,
                auth,
                args.create_table,
                specs,
                timestamp_type=args.timestamp_type,
                partition_by=args.partition_by,
            )
        if args.max_pending_rows > 0:
            throttle = _WalThrottle(
                args.questdb_url,
                auth,
                prefix,
                args.batch_size,
                args.max_pending_rows,
                args.resume_pending_rows,
                args.throttle_poll_secs,
            )
            log.info(
                "throttle: pausing feed when WAL backlog exceeds ~%s pending rows "
                "(resume at ~%s)",
                "{:,}".format(args.max_pending_rows),
                "{:,}".format(
                    args.resume_pending_rows
                    if args.resume_pending_rows > 0
                    else max(1, args.max_pending_rows // 2)
                ),
            )
            feeder = _ThrottledFeeder(
                args.questdb_url,
                args.batch_size,
                auth,
                throttle,
                check_every=args.throttle_check_every,
            )
        else:
            feeder = _IlpHttpFeeder(args.questdb_url, args.batch_size, auth)

    # Wrap the input so we can log a heartbeat without buffering the stream.
    t0 = time.monotonic()
    state = {"n": 0, "last": 0.0}

    def ticking(src):
        for line in src:
            state["n"] += 1
            if state["n"] % 200_000 == 0:
                now = time.monotonic()
                if now - state["last"] >= 2.0:
                    state["last"] = now
                    el = now - t0
                    rate = state["n"] / el if el > 0 else 0.0
                    log.info(
                        "progress: read %s field-lines | %s lines/s",
                        "{:,}".format(state["n"]),
                        "{:,}".format(int(rate)),
                    )
            yield line

    points, field_lines = merge_stream(ticking(sys.stdin), prefix, feeder, interval_ns)
    verb = "[dry-run] would write" if args.dry_run else "wrote"
    log.info(
        "%s %s %s points from %s field-lines (%.1fx reduction)",
        verb,
        "{:,}".format(points),
        "downsampled" if interval_ns else "wide",
        "{:,}".format(field_lines),
        (field_lines / points) if points else 0.0,
    )
    return 0


def _parse_args(argv: Optional[List[str]]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="pivot_lp.py",
        description="Merge sorted single-field LP into wide points and feed QuestDB.",
    )
    p.add_argument("--prefix", default="", help="measurement prefix, e.g. 'batmon_tele_'")
    p.add_argument("--no-prefix", action="store_true")
    p.add_argument(
        "--downsample",
        default="0",
        help="downsample onto a fixed time grid applied to ALL fields/columns: "
        "bucket size as Ns/Nm/Nh/Nd or bare nanoseconds (e.g. 10s, 20s, 1m). "
        "Keeps the last value per field per bucket; 0/none = exact pivot. "
        "REQUIRES timestamp-first sorted input (sort -k3,3n -k1,1).",
    )
    p.add_argument("--questdb-url", default="http://localhost:9000")
    p.add_argument("--batch-size", type=int, default=10_000)
    p.add_argument(
        "--create-table",
        default="",
        help="pre-create this table (CREATE TABLE IF NOT EXISTS) BEFORE feeding, "
        "with the designated timestamp + any --index columns. Other columns "
        "auto-create from the ILP feed. Needed because ILP auto-create never "
        "adds indexes. Single table only (the wide-pivot target).",
    )
    p.add_argument(
        "--index",
        default="",
        help="opt-in SYMBOL indexes for --create-table: comma list of tag "
        "columns, each optionally 'col:capacity' (e.g. 'did,uid,addrh:2048'). "
        "QuestDB has no composite index, so each gets its own. Only name tag "
        "columns -- a field named here would be forced to SYMBOL and break ILP.",
    )
    p.add_argument(
        "--timestamp-type",
        default="TIMESTAMP",
        help="designated timestamp type for --create-table: TIMESTAMP (us) or "
        "TIMESTAMP_NS (ns). Default TIMESTAMP.",
    )
    p.add_argument(
        "--partition-by",
        default="DAY",
        help="PARTITION BY unit for --create-table (default DAY).",
    )
    p.add_argument(
        "--max-pending-rows",
        type=int,
        default=0,
        help="throttle: pause feeding when QuestDB's WAL apply backlog "
        "(sequencerTxn - writerTxn, estimated as pending-txns * batch-size) for "
        "the target tables exceeds this many rows. 0 = no throttle. "
        "E.g. 10000000 keeps the un-applied backlog under ~10M rows.",
    )
    p.add_argument(
        "--resume-pending-rows",
        type=int,
        default=0,
        help="throttle low-watermark: once paused, resume only when the backlog "
        "drains below this many rows (hysteresis). 0 = half of --max-pending-rows.",
    )
    p.add_argument(
        "--throttle-poll-secs",
        type=float,
        default=5.0,
        help="throttle: seconds between backlog re-checks while paused (default 5).",
    )
    p.add_argument(
        "--throttle-check-every",
        type=int,
        default=20,
        help="throttle: check the backlog every N batches while feeding (default "
        "20, i.e. every 20*batch-size rows).",
    )
    p.add_argument("--user", default="")
    p.add_argument("--password", default="")
    p.add_argument("--token", default="")
    p.add_argument("--dry-run", action="store_true", help="count, POST nothing")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
