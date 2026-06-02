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
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable, Dict, List, Optional

from bulk_v1 import _IlpHttpFeeder, parse_basic_or_token_auth, rewrite_line
from csv_pivot import CsvSink
from qdb_admin import (
    IndexSpecError,
    TableSchema,
    ensure_full_table,
    ensure_indexed_table,
    fetch_table_schema,
    parse_index_spec,
    parse_schema_columns,
    parse_schema_tables,
)

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


class SchemaCoercer:
    """Allow-lists and type-coerces LP field tokens against a parsed schema.

    Built from ``{measurement: {column: kind}}`` (kind in
    ``bool``/``int``/``float``/``str``, as produced by
    :func:`qdb_admin.parse_schema_columns`, keyed by source MEASUREMENT rather
    than table). For each field token it:

    * drops the column if it is absent from that measurement's schema (so
      spurious source fields -- e.g. ``temperatures_8..254`` -- never reach the
      server and cannot auto-create columns);
    * coerces a BOOLEAN column to ``t``/``f`` and an INT/LONG column to ``Ni``
      (ILP refuses to write a float into a BOOLEAN/INT column and rejects the
      whole row, so the value MUST already match the column type);
    * strips a stray integer ``i`` suffix from a float column, and passes
      ``str`` columns through verbatim.

    It warns at most ONCE PER (measurement, column): once the first time a column
    is dropped, and once when a value does not cleanly match its declared type
    (a non-0/1 number coerced to BOOLEAN, a fractional value truncated to INT, or
    an unparseable/string value that gets dropped). A measurement with no schema
    entry is passed through unchanged.
    """

    def __init__(self, by_measurement: Dict[str, Dict[str, str]]):
        self._schema = by_measurement
        self._warned = set()
        self.dropped_cols = set()

    def _warn_once(self, key, msg, *args) -> None:
        if key not in self._warned:
            self._warned.add(key)
            log.warning(msg, *args)

    @staticmethod
    def _to_number(value: str) -> float:
        # LP integer fields carry a trailing 'i'; strip it before parsing.
        if value and value[-1] in "iI":
            value = value[:-1]
        return float(value)

    def _coerce_token(self, measurement: str, cols: Dict[str, str], tok: str) -> Optional[str]:
        """Filter/coerce ONE ``name=value`` token against ``cols``.

        Returns the (possibly rewritten) token, or None when the column is not
        in the schema (dropped) or the value cannot be coerced to its declared
        type. ``cols`` is passed in so the caller resolves the per-measurement
        schema dict once per line instead of once per token. A token without an
        ``=`` (no field value) passes through verbatim.
        """
        name, eq, value = tok.partition("=")
        if not eq:
            return tok
        kind = cols.get(name)
        if kind is None:
            if (measurement, name) not in self.dropped_cols:
                self.dropped_cols.add((measurement, name))
                log.info(
                    "schema: dropping column not in schema [%s.%s]",
                    measurement,
                    name,
                )
            return None
        if kind == "bool":
            coerced = self._to_bool(measurement, name, value)
        elif kind == "int":
            coerced = self._to_int(measurement, name, value)
        elif kind == "float":
            coerced = self._to_float(value)
        else:
            return tok
        if coerced is None:
            return None
        # Avoid rebuilding the token string when the value was unchanged (the
        # common case for already-canonical int/float literals): return the
        # original token instead of re-concatenating name + "=" + value.
        return tok if coerced is value else name + "=" + coerced

    def compile_token_coercer(self, measurement: str, cols: Dict[str, str]) -> "Callable[[str], Optional[str]]":
        """Build a fast single-token coercer specialized for one measurement.

        The hot path (downsample/exact merge over long-format input) calls the
        returned closure once per field-line. It produces output byte-identical
        to :meth:`_coerce_token` but avoids that method's per-token costs:

        * no ``str.partition`` (which allocates a 3-tuple + 2 substrings every
          line) -- a single ``find("=")`` locates the name/value split, and the
          name is sliced only when a dict lookup is actually needed;
        * no secondary bound-method dispatch to ``_to_int``/``_to_bool``/
          ``_to_float`` -- the dominant INT canonical-``Ni`` case is fully inline
          and returns the ORIGINAL token object (no re-concatenation), so the
          common line costs one ``find`` + one ``dict.get`` + a tight digit check;
        * all attribute lookups it does need (the schema dict, the warn/drop
          sets, the slow-path fallbacks) are hoisted to closure locals so the
          per-line body touches no ``self.`` attributes.

        The rarer BOOL/STR kinds and any non-canonical INT delegate to the
        unchanged methods, so warn-once, drop-once, NULL/None and error handling
        are identical to the slow path. FLOAT is inlined (its only normalization
        is stripping a trailing integer marker).
        """
        # Hoist everything the body might touch into closure-local fast cells.
        cols_get = cols.get
        dropped = self.dropped_cols
        to_bool = self._to_bool
        to_int = self._to_int

        def coerce(tok: str) -> Optional[str]:
            eq = tok.find("=")
            if eq < 0:
                return tok
            name = tok[:eq]
            kind = cols_get(name)
            if kind is None:
                if (measurement, name) not in dropped:
                    dropped.add((measurement, name))
                    log.info(
                        "schema: dropping column not in schema [%s.%s]",
                        measurement,
                        name,
                    )
                return None
            if kind == "int":
                # Inline canonical-Ni fast path (the dominant column kind for
                # this workload, and most values already arrive canonical). Must
                # stay byte-identical to _to_int's fast path: optional leading
                # '-', all digits, no leading zero (except a lone '0'), no '-0',
                # trailing 'i'. On a hit, return the ORIGINAL token unchanged.
                #
                # Slice the magnitude straight out of ``tok`` using the known '='
                # offset so the canonical case allocates only ``name`` (needed for
                # the dict lookup) plus the digit slice the validation requires --
                # not a separate ``value`` string.
                last = tok[-1]
                if last == "i" or last == "I":
                    # digits = chars between '=' and the trailing i/I marker.
                    dstart = eq + 1
                    neg = tok[dstart:dstart + 1] == "-"
                    mag = tok[dstart + 1:-1] if neg else tok[dstart:-1]
                    if mag.isdigit() and (mag == "0" or mag[0] != "0") and not (neg and mag == "0"):
                        # _to_int always emits a lowercase 'i'. When the source
                        # already used 'i', the canonical token IS `tok` -- return
                        # it unchanged (no allocation). An uppercase 'I' source is
                        # still canonical numerically but must be rewritten to 'i'
                        # to match _to_int byte-for-byte.
                        return tok if last == "i" else tok[:-1] + "i"
                coerced = to_int(measurement, name, tok[eq + 1:])
                if coerced is None:
                    return None
                return name + "=" + coerced
            if kind == "float":
                # value -> FLOAT: _to_float's ONLY change is stripping a trailing
                # integer marker 'i'/'I' (an int source into a float column;
                # lossless, no warning). tok is name+"="+value, so tok[:-1] equals
                # name+"="+value[:-1] exactly -- byte-identical to the old
                # name + "=" + _to_float(value), but with no value slice and no
                # call (FLOAT is the dominant coerced kind on this workload).
                last = tok[-1]
                return tok[:-1] if last == "i" or last == "I" else tok
            value = tok[eq + 1:]
            if kind == "bool":
                coerced = to_bool(measurement, name, value)
            else:
                return tok
            if coerced is None:
                return None
            return tok if coerced is value else name + "=" + coerced

        return coerce

    def transform_fields(self, measurement: str, fields: str) -> Optional[str]:
        """Return the filtered/coerced field set, or None if nothing survives."""
        cols = self._schema.get(measurement)
        if cols is None:
            return fields
        out: List[str] = []
        for tok in fields.split(","):
            coerced = self._coerce_token(measurement, cols, tok)
            if coerced is not None:
                out.append(coerced)
        return ",".join(out) if out else None

    def _to_bool(self, measurement: str, name: str, value: str) -> Optional[str]:
        low = value.lower()
        if low in ("t", "true"):
            return "t"
        if low in ("f", "false"):
            return "f"
        try:
            number = self._to_number(value)
        except ValueError:
            self._warn_once(
                (measurement, name, "bool"),
                "schema: %s.%s is BOOLEAN but value %r is not 0/1/t/f; dropping",
                measurement,
                name,
                value,
            )
            return None
        if number not in (0.0, 1.0):
            self._warn_once(
                (measurement, name, "bool"),
                "schema: %s.%s is BOOLEAN but value %s is not 0/1; nonzero->t",
                measurement,
                name,
                value,
            )
        return "t" if number != 0.0 else "f"

    def _to_int(self, measurement: str, name: str, value: str) -> Optional[str]:
        # Fast path: a value already in LP integer form (optional '-', all
        # digits, trailing 'i') is the common case for INT/LONG columns from
        # export-lp. It is already exactly what ILP wants, so skip the
        # float->int->str round trip (float parsing dominated the coercer's
        # CPU). lstrip('-') then isdigit() rejects '', '-', floats ('3.7'),
        # scientific notation, and non-numerics, which all fall through to the
        # slow, fully-validating path below.
        if value and (value[-1] == "i" or value[-1] == "I"):
            digits = value[:-1]
            mag = digits[1:] if (digits[:1] == "-") else digits
            # Require canonical form (no leading zeros, except a lone "0") so the
            # fast path's output is byte-identical to str(int(...)); anything
            # else (floats, '+', leading zeros, empty) falls through to the slow
            # path that normalizes and warns as before.
            if mag.isdigit() and (mag == "0" or mag[0] != "0") and digits != "-0":
                return digits + "i"
        try:
            number = self._to_number(value)
        except ValueError:
            self._warn_once(
                (measurement, name, "int"),
                "schema: %s.%s is INT/LONG but value %r is not numeric; dropping",
                measurement,
                name,
                value,
            )
            return None
        truncated = int(number)
        if float(truncated) != number:
            self._warn_once(
                (measurement, name, "int"),
                "schema: %s.%s is INT/LONG but value %s is fractional; truncating",
                measurement,
                name,
                value,
            )
        return str(truncated) + "i"

    @staticmethod
    def _to_float(value: str) -> str:
        # An integer source field (Ni) into a FLOAT column: drop the 'i' so ILP
        # reads it as a float. int->float is lossless, so no warning.
        if value and value[-1] in "iI":
            return value[:-1]
        return value


def merge_stream(
    lines,
    prefix: str,
    feeder: Optional["_IlpHttpFeeder"],
    interval_ns: int = 0,
    coercer: Optional["SchemaCoercer"] = None,
    on_progress: Optional["Callable[[int], None]"] = None,
    sink=None,
):
    """Merge single-field LP lines into wide points; feed or count.

    Returns (points, field_lines).

    Each merged point is handed to a sink as ``emit(head, field_tokens, ts)``
    where ``head`` is ``measurement,tags`` (unprefixed), ``field_tokens`` is the
    list of ``name=value`` field strings, and ``ts`` is the epoch timestamp. The
    default sink (when ``sink`` is None) rebuilds the prefixed line-protocol line
    and feeds it to ``feeder`` -- the original behaviour. A CSV sink
    (:class:`csv_pivot.CsvSink`) instead writes a wide CSV row per point. ``sink``
    takes precedence over ``feeder``/``prefix`` when given.

    ``on_progress``, if given, is called with the running field-line count once
    every 200k lines -- a cheap heartbeat folded into the existing loop counter
    so there is no per-line generator/dict overhead (the wrapper it replaces was
    ~38% of pivot CPU). The callback itself rate-limits its own logging.

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

    if sink is not None:
        emit = sink.emit
        finish = sink.close
    else:
        def emit(head, tokens, ts):
            out = rewrite_line(head + " " + ",".join(tokens) + " " + ts, prefix)
            if feeder is not None:
                feeder.add(out)

        def finish():
            if feeder is not None:
                feeder.flush()

    # The source measurement of a head (``measurement,tags``) is the substring
    # before the first comma -- constant per series and, in this workload, the
    # same one or two measurements across billions of lines. Cache head ->
    # (measurement, cols) so the per-line coercion costs a single dict hit (on
    # the head) instead of a ``head.split(",", 1)[0]`` AND a second schema
    # lookup. ``cols`` is the per-measurement column map (or None when the
    # measurement has no schema, i.e. pass tokens through unchanged); resolving
    # it here lets the loop hand it straight to the coercer's per-token path.
    # head -> (measurement, cols, coerce) where ``coerce`` is the compiled
    # single-token closure for that measurement (or None when the measurement has
    # no schema). Compiling once per measurement lets the per-line hot path call
    # one closure with all method/attribute lookups already hoisted to locals,
    # instead of going through _coerce_token's per-token bound-method dispatch.
    head_cols: Dict[str, tuple] = {}
    _coercer_schema = coercer._schema if coercer is not None else None

    def cols_for_head(head: str) -> tuple:
        hit = head_cols.get(head)
        if hit is None:
            c = head.find(",")
            meas = head if c < 0 else head[:c]
            cols = _coercer_schema.get(meas) if _coercer_schema is not None else None
            coerce = (
                coercer.compile_token_coercer(meas, cols)
                if coercer is not None and cols is not None
                else None
            )
            hit = (meas, cols, coerce)
            head_cols[head] = hit
        return hit

    # Hoist the cache's bound ``get`` so the per-line common case (a cache hit)
    # is a single C-level dict lookup, not a Python ``cols_for_head`` call frame.
    _hc_get = head_cols.get

    if interval_ns:
        cur_bucket: Optional[int] = None
        acc: Dict[str, Dict[str, str]] = {}  # head -> {field_name: "name=value"}

        def flush_bucket():
            nonlocal points
            if cur_bucket is None:
                return
            ts_label = str(cur_bucket + interval_ns)
            for head, fmap in acc.items():
                if not fmap:
                    continue
                emit(head, fmap.values(), ts_label)
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
            if on_progress is not None and field_lines % 200_000 == 0:
                on_progress(field_lines)
            if cur_bucket is None:
                cur_bucket = bucket
            elif bucket < cur_bucket:
                # Downsample's single-open-bucket merge is ONLY correct when the
                # input timestamps are non-decreasing. A regression means the
                # input is not time-sorted (e.g. raw series-major export-lp fed
                # straight in). Without this guard the merge silently emits one
                # fragmented row per field instead of one merged row per bucket
                # -- corruption with no error. Fail loudly instead.
                raise ValueError(
                    "downsample requires timestamp-sorted input, but timestamp %s "
                    "falls before the open bucket %d. Sort the export first "
                    "(timestamp-major), e.g. the awk-prepend sort in "
                    "import_batmon_copy.sh." % (ts, cur_bucket)
                )
            elif bucket != cur_bucket:
                # ts is non-decreasing, so the prior bucket is now complete.
                flush_bucket()
                cur_bucket = bucket
            fmap = acc.get(head)
            if fmap is None:
                fmap = {}
                acc[head] = fmap
            # Long-format input (export-lp) puts one field per line, so ``fields``
            # has no comma in the overwhelmingly common case: coerce and store
            # the single token directly, skipping the split(",")/join round trip
            # done for the rare multi-field line.
            if "," not in fields:
                if coercer is not None:
                    # Inline the head cache lookup: a hit (the steady state, one
                    # entry per distinct series) avoids the per-line
                    # cols_for_head() call frame entirely.
                    hit = _hc_get(head)
                    coerce = hit[2] if hit is not None else cols_for_head(head)[2]
                    if coerce is not None:
                        fields = coerce(fields)
                        if fields is None:
                            continue
                eq = fields.find("=")
                fmap[fields if eq < 0 else fields[:eq]] = fields
            else:
                if coercer is not None:
                    meas, _, _ = cols_for_head(head)
                    tfields = coercer.transform_fields(meas, fields)
                    if tfields is None:
                        continue
                    fields = tfields
                for tok in fields.split(","):
                    fmap[tok.split("=", 1)[0]] = tok
        flush_bucket()
        finish()
        return points, field_lines

    cur_key = None
    cur_head: Optional[str] = None
    cur_ts: Optional[str] = None
    cur_fields: List[str] = []

    def flush_point():
        nonlocal points
        if cur_head is None or cur_ts is None or not cur_fields:
            return
        emit(cur_head, cur_fields, cur_ts)
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
        if on_progress is not None and field_lines % 200_000 == 0:
            on_progress(field_lines)
        if coercer is not None:
            if "," not in fields:
                hit = _hc_get(head)
                coerce = hit[2] if hit is not None else cols_for_head(head)[2]
                if coerce is not None:
                    fields = coerce(fields)
            else:
                meas = cols_for_head(head)[0]
                fields = coercer.transform_fields(meas, fields)
        key = (head, ts)
        if key != cur_key:
            flush_point()
            cur_head, cur_key, cur_ts = head, key, ts
            cur_fields = [fields] if fields else []
        else:
            if fields:
                cur_fields.append(fields)
    flush_point()
    finish()
    return points, field_lines


def _make_on_progress() -> "Callable[[int], None]":
    """Build a heartbeat callback for merge_stream that self-limits to once/2s."""
    t0 = time.monotonic()
    last = [0.0]

    def on_progress(n: int) -> None:
        now = time.monotonic()
        if now - last[0] >= 2.0:
            last[0] = now
            el = now - t0
            rate = n / el if el > 0 else 0.0
            log.info(
                "progress: read %s field-lines | %s lines/s",
                "{:,}".format(n),
                "{:,}".format(int(rate)),
            )

    return on_progress


def _run_csv(args: argparse.Namespace, prefix: str, interval_ns: int) -> int:
    """CSV-emit mode: pivot into per-measurement wide CSVs for QuestDB COPY.

    Resolves each measurement's schema from the --schema-file first, then (unless
    --no-csv-schema-from-table) by reading an EXISTING table's schema from
    QuestDB. A measurement with neither is skipped. The ILP-targeting SchemaCoercer
    is NOT used here -- CsvSink does its own per-column, CSV-shaped formatting and
    the schema column set is the allow-list (unknown fields are simply not emitted).
    """
    file_schemas: Dict[str, TableSchema] = {}
    if args.schema_file:
        try:
            with open(args.schema_file, encoding="utf-8") as fh:
                tables = parse_schema_tables(fh.read())
        except OSError as exc:
            log.error("cannot read --schema-file %s: %s", args.schema_file, exc)
            return 2
        if not tables:
            log.error("no CREATE TABLE statements found in %s", args.schema_file)
            return 2
        for table, schema in tables.items():
            meas = table[len(prefix):] if prefix and table.startswith(prefix) else table
            file_schemas[meas] = schema
        log.info(
            "csv: schema for %d measurement(s) from %s: %s",
            len(file_schemas),
            args.schema_file,
            sorted(file_schemas),
        )

    auth = parse_basic_or_token_auth(args.user, args.password, args.token)

    if args.csv_create_tables and file_schemas:
        # Pre-create complete, empty, partitioned targets so a later COPY accepts
        # the CSV header. Uses each table's own declared types (the schema file is
        # authoritative); idempotent CREATE TABLE IF NOT EXISTS leaves an existing
        # table untouched.
        for schema in file_schemas.values():
            ensure_full_table(args.questdb_url, auth, schema)

    def resolve(measurement: str) -> Optional[TableSchema]:
        schema = file_schemas.get(measurement)
        if schema is not None:
            return schema
        if args.csv_schema_from_table:
            return fetch_table_schema(args.questdb_url, auth, prefix + measurement)
        return None

    os.makedirs(args.csv_out_dir, exist_ok=True)
    sink = CsvSink(
        args.csv_out_dir,
        resolve,
        prefix=prefix,
        delimiter=args.csv_delimiter,
        timestamp_mode=args.csv_timestamp_mode,
    )
    points, field_lines = merge_stream(
        sys.stdin, prefix, None, interval_ns, None, _make_on_progress(), sink
    )
    log.info(
        "csv: wrote %s %s points to %d file(s) from %s field-lines (%.1fx reduction)",
        "{:,}".format(points),
        "downsampled" if interval_ns else "wide",
        len(sink.paths),
        "{:,}".format(field_lines),
        (field_lines / points) if points else 0.0,
    )
    return 0


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
    if args.csv_out_dir:
        return _run_csv(args, prefix, interval_ns)
    if args.index and not args.create_table:
        log.error("--index requires --create-table NAME (the table to pre-create)")
        return 2

    coercer: Optional[SchemaCoercer] = None
    if args.schema_file:
        try:
            with open(args.schema_file, encoding="utf-8") as fh:
                tables = parse_schema_columns(fh.read())
        except OSError as exc:
            log.error("cannot read --schema-file %s: %s", args.schema_file, exc)
            return 2
        if not tables:
            log.error("no CREATE TABLE statements found in %s", args.schema_file)
            return 2
        by_measurement = {
            (table[len(prefix):] if prefix and table.startswith(prefix) else table): cols
            for table, cols in tables.items()
        }
        coercer = SchemaCoercer(by_measurement)
        log.info(
            "schema: enforcing %d table(s) from %s on measurements %s",
            len(tables),
            args.schema_file,
            sorted(by_measurement),
        )

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

    # Heartbeat without buffering the stream: merge_stream calls this every 200k
    # field-lines (folded into its existing counter -- no per-line wrapper).
    points, field_lines = merge_stream(
        sys.stdin, prefix, feeder, interval_ns, coercer, _make_on_progress()
    )
    if coercer is not None and coercer.dropped_cols:
        log.info(
            "schema: dropped %d distinct spurious column(s) not in the schema",
            len(coercer.dropped_cols),
        )
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
        "--csv-out-dir",
        default="",
        help="CSV mode: write wide rows to one CSV file per measurement in this "
        "directory (the QuestDB COPY input root) instead of feeding ILP. The file "
        "is named <prefix><measurement>.csv. Column set/order/types come from "
        "--schema-file, or (per measurement) from an existing table's schema. "
        "Feed these to QuestDB's parallel COPY -- it sorts each partition itself, "
        "so the output need not be time-ordered.",
    )
    p.add_argument(
        "--csv-delimiter",
        default=",",
        help="CSV field delimiter for --csv-out-dir (default ',').",
    )
    p.add_argument(
        "--csv-timestamp-mode",
        default="iso-ns",
        choices=("iso-ns", "epoch-ns"),
        help="CSV timestamp encoding: 'iso-ns' writes "
        "yyyy-MM-ddTHH:mm:ss.SSSSSSSSSZ (match with COPY FORMAT); 'epoch-ns' "
        "writes the raw nanosecond integer. Default iso-ns.",
    )
    p.add_argument(
        "--csv-create-tables",
        action="store_true",
        help="CSV mode: pre-create each --schema-file table (complete, empty, "
        "partitioned, with DEDUP UPSERT KEYS) before writing, so a later COPY has "
        "a valid target. Off by default (the orchestrator usually does this).",
    )
    p.add_argument(
        "--no-csv-schema-from-table",
        dest="csv_schema_from_table",
        action="store_false",
        help="CSV mode: do NOT fall back to reading an existing table's schema "
        "from QuestDB for a measurement absent from --schema-file (default: do).",
    )
    p.set_defaults(csv_schema_from_table=True)
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
        "--schema-file",
        default="",
        help="path to a CREATE TABLE schema (e.g. tables.sql). Enforces it on "
        "the feed: drops any source field NOT in the schema (so spurious columns "
        "cannot auto-create) and coerces values to the declared type -- BOOLEAN "
        "to t/f, INT/LONG to integer (Ni). The source measurement maps to a table "
        "by adding --prefix. Warns once per column on a type mismatch.",
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
