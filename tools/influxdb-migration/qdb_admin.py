#!/usr/bin/env python3
"""Pre-create QuestDB tables (with optional SYMBOL indexes) before an ILP feed.

ILP auto-create NEVER adds indexes: to get an indexed SYMBOL column the table
must already exist with that column declared ``SYMBOL INDEX`` BEFORE the first
write. This module builds an idempotent ``CREATE TABLE IF NOT EXISTS`` carrying
the designated timestamp plus the requested indexed tag columns, and POSTs it to
QuestDB's ``/exec`` endpoint. Every other tag/field column is still auto-created
by the subsequent ILP feed, so only the columns you actually want indexed need
to be named here.

Why opt-in and not "index every tag": a QuestDB SYMBOL column is already
dictionary-encoded, so a non-indexed equality filter is a cheap vectorized scan
(pruned by the timestamp partition). An index only pays off for a SELECTIVE
filter on a LARGE table, and it costs disk plus per-commit maintenance --
expensive for high-cardinality tags. QuestDB also has no composite (multi-column)
index, so each named column gets its own single-column index.

The pure builders (``parse_index_spec``, ``build_create_table_ddl``) carry no I/O
so the test suite can exercise the DDL generation without a live QuestDB.
"""

from __future__ import annotations

import logging
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import List, Optional, Tuple

log = logging.getLogger("influx_migrate.qdb_admin")

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_VALID_TIMESTAMP_TYPES = ("TIMESTAMP", "TIMESTAMP_NS")

# (column, capacity-or-None)
IndexSpec = Tuple[str, Optional[int]]


class IndexSpecError(ValueError):
    """Raised when an --index spec is malformed (bad name, capacity, dup)."""


def parse_index_spec(spec: Optional[str]) -> List[IndexSpec]:
    """Parse ``'col[:capacity],...'`` into ``[(col, capacity_or_None), ...]``.

    ``capacity``, if present, must be a positive integer (the SYMBOL index
    CAPACITY hint). Blank entries are skipped; an empty/invalid column name, a
    non-integer or non-positive capacity, or a duplicate column all raise
    :class:`IndexSpecError`. Order is preserved.
    """
    out: List[IndexSpec] = []
    seen = set()
    for raw in (spec or "").split(","):
        item = raw.strip()
        if not item:
            continue
        if ":" in item:
            name, _, cap_str = item.partition(":")
            name, cap_str = name.strip(), cap_str.strip()
            try:
                capacity: Optional[int] = int(cap_str)
            except ValueError:
                raise IndexSpecError(
                    "index capacity for %r must be an integer, got %r"
                    % (name, cap_str)
                )
            if capacity <= 0:
                raise IndexSpecError(
                    "index capacity for %r must be positive, got %d"
                    % (name, capacity)
                )
        else:
            name, capacity = item, None
        if not _IDENT.match(name):
            raise IndexSpecError(
                "index column %r is not a simple [A-Za-z_][A-Za-z0-9_]* identifier"
                % name
            )
        if name in seen:
            raise IndexSpecError("duplicate index column %r" % name)
        seen.add(name)
        out.append((name, capacity))
    return out


def build_create_table_ddl(
    table: str,
    index_specs: List[IndexSpec],
    timestamp_col: str = "timestamp",
    timestamp_type: str = "TIMESTAMP",
    partition_by: str = "DAY",
    wal: bool = True,
) -> str:
    """Build an idempotent CREATE TABLE for ``table`` with the indexed columns.

    Only the designated timestamp and the indexed SYMBOL columns are declared;
    the ILP feed auto-creates the rest. ``timestamp_type`` must be ``TIMESTAMP``
    (microseconds) or ``TIMESTAMP_NS`` (nanoseconds). Raises
    :class:`IndexSpecError` on an unknown timestamp type or empty table name.
    """
    if not table or not table.strip():
        raise IndexSpecError("table name is required")
    ts_type = timestamp_type.strip().upper()
    if ts_type not in _VALID_TIMESTAMP_TYPES:
        raise IndexSpecError(
            "timestamp_type must be one of %s, got %r"
            % (", ".join(_VALID_TIMESTAMP_TYPES), timestamp_type)
        )
    cols = ["    %s %s" % (timestamp_col, ts_type)]
    for name, capacity in index_specs:
        cap_clause = " CAPACITY %d" % capacity if capacity else ""
        cols.append("    %s SYMBOL INDEX%s" % (name, cap_clause))
    wal_clause = " WAL" if wal else ""
    return (
        "CREATE TABLE IF NOT EXISTS '%s' (\n%s\n) timestamp(%s) PARTITION BY %s%s"
        % (table, ",\n".join(cols), timestamp_col, partition_by, wal_clause)
    )


def ensure_indexed_table(
    base_url: str,
    auth: Optional[str],
    table: str,
    index_specs: List[IndexSpec],
    timestamp_col: str = "timestamp",
    timestamp_type: str = "TIMESTAMP",
    partition_by: str = "DAY",
    wal: bool = True,
    timeout: float = 60.0,
) -> str:
    """Build and execute the CREATE TABLE on QuestDB's ``/exec`` (idempotent).

    Returns the DDL that was run. Raises SystemExit on an HTTP/transport error so
    a misconfigured pre-create aborts the import loudly rather than silently
    feeding into an unindexed auto-created table. ``CREATE TABLE IF NOT EXISTS``
    makes a second run a no-op, and an existing table is left untouched (so it
    will NOT add the index to a table that already exists without it -- see the
    README note).
    """
    ddl = build_create_table_ddl(
        table, index_specs, timestamp_col, timestamp_type, partition_by, wal
    )
    url = base_url.rstrip("/") + "/exec?" + urllib.parse.urlencode({"query": ddl})
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", auth)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace").strip()
        raise SystemExit(
            "QuestDB rejected CREATE TABLE for %r (HTTP %d): %s"
            % (table, exc.code, detail)
        ) from exc
    except urllib.error.URLError as exc:
        raise SystemExit(
            "QuestDB /exec unreachable for CREATE TABLE: %s" % exc.reason
        ) from exc
    cols = ", ".join(
        "%s%s" % (n, "(cap %d)" % c if c else "") for n, c in index_specs
    )
    log.info("ensured table %s with indexed columns: %s", table, cols or "(none)")
    return ddl
