#!/usr/bin/env python3
"""Wide-CSV sink for the pivot, sized for QuestDB's parallel ``COPY`` import.

The pivot (:func:`pivot_lp.merge_stream`) merges InfluxDB's one-field-per-line
export into wide points and hands each point to a sink. The ILP sink rebuilds a
line-protocol line and POSTs it; this CSV sink instead writes one wide CSV row per
point into a per-measurement file under the QuestDB ``COPY`` input root.

Why CSV + COPY rather than ILP: QuestDB's ``COPY`` (``ParallelCsvFileImporter``)
is built for UNORDERED input -- it sorts each partition by timestamp in parallel
and writes column files directly, bypassing the ILP re-parse, the WAL sequencer,
and out-of-order partition rewrites. So this sink does NOT need its output
time-ordered; it only needs every field of one ``(tagset, timestamp/bucket)``
merged into one row, which the pivot already guarantees.

The column set, order, and per-column type come from a :class:`qdb_admin.TableSchema`
per measurement (parsed from a schema file or read back from an existing table).
A source field absent from the schema is simply not in the header, so it is
dropped (the schema is the allow-list); a schema column with no value in a row
becomes an empty cell, which COPY reads as NULL.

Value formatting per column kind: integer/float fields shed line protocol's
trailing ``i``; booleans normalise to ``true``/``false``; string fields shed line
protocol's surrounding double quotes and ``\\``-escaping, then get RFC-4180 CSV
quoting. The designated timestamp is emitted either as a fixed-width ISO-8601
nanosecond string (``yyyy-MM-ddTHH:mm:ss.SSSSSSSSSZ``) or as the raw epoch-ns
integer, selectable to match how the COPY statement parses it.

Like the rest of the tool, this assumes field/tag values do not contain commas
(the field set is comma-delimited and split naively); the numeric telemetry this
targets satisfies that. Spaces in string values ARE tolerated (the pivot splits
the head/fields/timestamp on the first/last space).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Callable, Dict, List, Optional, TextIO, Tuple

from bulk_v1 import _measurement_end, _unescape_measurement
from qdb_admin import TableSchema

log = logging.getLogger("influx_migrate.csv")

_CSV_SPECIALS = set(',"\n\r')


def _split_unescaped(s: str, delim: str):
    """Yield segments of ``s`` split on ``delim`` chars not escaped by backslash.

    A backslash escapes the next character (line protocol's escaping rule for
    tag keys/values), so a ``\\,`` inside a tag value does not split. The
    backslashes are left in place; the caller unescapes each segment.
    """
    start = 0
    escaped = False
    for i, ch in enumerate(s):
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == delim:
            yield s[start:i]
            start = i + 1
    yield s[start:]


def parse_head(head: str) -> Tuple[str, List[Tuple[str, str]]]:
    """Split an LP head ``measurement,tag1=v1,tag2=v2`` into (measurement, tags).

    The measurement and every tag key/value are returned UNESCAPED (line protocol
    escaping collapsed). ``head`` is the portion of an LP line before the first
    space, so it has no field set or timestamp.
    """
    cut = _measurement_end(head)
    measurement = _unescape_measurement(head[:cut])
    tags: List[Tuple[str, str]] = []
    if cut < len(head):
        for seg in _split_unescaped(head[cut + 1 :], ","):
            if not seg:
                continue
            key, eq, value = seg.partition("=")
            if not eq:
                continue
            tags.append((_unescape_measurement(key), _unescape_measurement(value)))
    return measurement, tags


def _unescape_lp_string(value: str) -> str:
    """Strip line protocol's surrounding double quotes and ``\\`` escaping.

    A string field is written as ``"..."`` with ``\\"`` for an internal quote and
    ``\\\\`` for a backslash. A non-quoted value (numeric/bool) is returned
    unchanged so this is safe to call on any token.
    """
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        inner = value[1:-1]
        out: List[str] = []
        escaped = False
        for ch in inner:
            if escaped:
                out.append(ch)
                escaped = False
            elif ch == "\\":
                escaped = True
            else:
                out.append(ch)
        if escaped:
            out.append("\\")
        return "".join(out)
    return value


def _csv_quote(value: str, delimiter: str) -> str:
    """RFC-4180 quote ``value`` if it contains the delimiter, a quote, or newline."""
    if value and (delimiter in value or any(c in _CSV_SPECIALS for c in value)):
        return '"' + value.replace('"', '""') + '"'
    return value


def format_ts_iso_ns(ts_ns: int) -> str:
    """Format an epoch-nanosecond integer as ``yyyy-MM-ddTHH:mm:ss.SSSSSSSSSZ``.

    Python's ``datetime`` only carries microseconds, so the 9-digit nanosecond
    fraction is composed by hand from the sub-second remainder.
    """
    secs, nanos = divmod(ts_ns, 1_000_000_000)
    tm = time.gmtime(secs)
    return "%04d-%02d-%02dT%02d:%02d:%02d.%09dZ" % (
        tm.tm_year,
        tm.tm_mon,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
        nanos,
    )


class _MeasurementWriter:
    """One open CSV file plus the ordered column names for its header."""

    def __init__(self, handle: TextIO, columns: List[str]):
        self.handle = handle
        self.columns = columns


class CsvSink:
    """Routes wide pivot points into per-measurement CSV files for ``COPY``.

    ``resolve(measurement)`` returns the :class:`qdb_admin.TableSchema` for a
    source MEASUREMENT name (prefix stripped), or None if unknown. It is called
    once per measurement and cached, so it can do I/O (e.g. read an existing
    table's schema from QuestDB) lazily as new measurements appear in the stream;
    pass a dict's ``.get`` for a static schema-file mapping. Each measurement's
    rows go to ``<out_dir>/<prefix><measurement>.csv`` (the COPY target table
    name), opened lazily with a header on first write. A measurement that resolves
    to None is skipped (warned once). Call :meth:`close` to flush and close every
    file and return the written paths.
    """

    def __init__(
        self,
        out_dir: str,
        resolve: "Callable[[str], Optional[TableSchema]]",
        prefix: str = "",
        delimiter: str = ",",
        timestamp_mode: str = "iso-ns",
    ):
        self._out_dir = out_dir
        self._resolve = resolve
        self._prefix = prefix
        self._delim = delimiter
        if timestamp_mode not in ("iso-ns", "epoch-ns"):
            raise ValueError(
                "timestamp_mode must be 'iso-ns' or 'epoch-ns', got %r" % timestamp_mode
            )
        self._ts_mode = timestamp_mode
        self._schemas: Dict[str, Optional[TableSchema]] = {}
        self._writers: Dict[str, _MeasurementWriter] = {}
        self._skipped: set = set()
        self.paths: Dict[str, str] = {}

    def _schema_for(self, measurement: str) -> "Optional[TableSchema]":
        if measurement in self._schemas:
            return self._schemas[measurement]
        schema = self._resolve(measurement)
        self._schemas[measurement] = schema
        return schema

    def _writer_for(
        self, measurement: str, schema: TableSchema
    ) -> _MeasurementWriter:
        writer = self._writers.get(measurement)
        if writer is not None:
            return writer
        table = self._prefix + measurement
        path = os.path.join(self._out_dir, table + ".csv")
        columns = [c.name for c in schema.columns]
        handle = open(path, "w", encoding="utf-8", newline="")
        handle.write(self._delim.join(columns) + "\n")
        writer = _MeasurementWriter(handle, columns)
        self._writers[measurement] = writer
        self.paths[measurement] = path
        log.info("csv: writing %s (%d columns) -> %s", table, len(columns), path)
        return writer

    def _format_ts(self, ts: str) -> str:
        try:
            ts_ns = int(ts)
        except ValueError:
            return ""
        if self._ts_mode == "epoch-ns":
            return str(ts_ns)
        return format_ts_iso_ns(ts_ns)

    def _format_value(self, kind: str, raw: str) -> str:
        if kind in ("int", "float"):
            # Line protocol integers carry a trailing 'i'; CSV wants a bare number.
            if raw and raw[-1] in "iI":
                raw = raw[:-1]
            return raw
        if kind == "bool":
            low = raw.lower()
            if low in ("t", "true"):
                return "true"
            if low in ("f", "false"):
                return "false"
            return raw
        # str / unknown: drop LP string quoting, then RFC-4180 quote for CSV.
        return _csv_quote(_unescape_lp_string(raw), self._delim)

    def emit(self, head: str, field_tokens, ts: str) -> None:
        """Write one wide CSV row for a merged point. Matches the sink protocol."""
        measurement, tags = parse_head(head)
        schema = self._schema_for(measurement)
        if schema is None:
            if measurement not in self._skipped:
                self._skipped.add(measurement)
                log.warning(
                    "csv: no schema for measurement %r; skipping its rows", measurement
                )
            return
        values: Dict[str, str] = {}
        for tok in field_tokens:
            for pair in tok.split(","):
                name, eq, val = pair.partition("=")
                if eq:
                    values[name] = val
        tag_map = dict(tags)
        writer = self._writer_for(measurement, schema)
        kinds = {c.name: c.kind for c in schema.columns}
        cells: List[str] = []
        for col in writer.columns:
            if col == schema.timestamp_col:
                cells.append(self._format_ts(ts))
                continue
            if col in tag_map:
                cells.append(_csv_quote(tag_map[col], self._delim))
            elif col in values:
                cells.append(self._format_value(kinds.get(col, "str"), values[col]))
            else:
                cells.append("")  # absent -> empty cell -> NULL in COPY
        writer.handle.write(self._delim.join(cells) + "\n")

    def close(self) -> Dict[str, str]:
        """Close all open files; return ``{measurement: path}`` of files written."""
        for writer in self._writers.values():
            writer.handle.close()
        return dict(self.paths)
