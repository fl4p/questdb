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

import json
import logging
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, NamedTuple, Optional, Tuple

log = logging.getLogger("influx_migrate.qdb_admin")

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_VALID_TIMESTAMP_TYPES = ("TIMESTAMP", "TIMESTAMP_NS")

# QuestDB column type -> the LP coercion "kind" the import pipeline enforces.
# Anything not listed (SYMBOL, STRING, VARCHAR, CHAR, TIMESTAMP, DATE, UUID,
# arrays, ...) maps to "str": passed through verbatim (tags arrive in the LP
# head, not as field tokens, so they are never coerced anyway).
_TYPE_KIND = {
    "BOOLEAN": "bool",
    "BYTE": "int",
    "SHORT": "int",
    "INT": "int",
    "LONG": "int",
    "FLOAT": "float",
    "DOUBLE": "float",
}

_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?['\"]?"
    r"([A-Za-z_][A-Za-z0-9_]*)['\"]?\s*\((.*?)\)\s*"
    r"timestamp\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)"
    r"(?:\s+PARTITION\s+BY\s+([A-Za-z]+))?",
    re.IGNORECASE | re.DOTALL,
)

# (column, capacity-or-None)
IndexSpec = Tuple[str, Optional[int]]


class ColumnDef(NamedTuple):
    """One parsed column: its name, the verbatim type/modifier text, and flags.

    ``definition`` is everything after the column name up to the comma (e.g.
    ``"SYMBOL INDEX CAPACITY 2048"`` or ``"DOUBLE"``), preserved verbatim so the
    complete-DDL builder re-emits the exact declared type without re-deriving it.
    ``kind`` is the LP coercion kind (see :data:`_TYPE_KIND`). ``is_symbol`` marks
    a tag column (filled from the LP tag set), as opposed to a field column.
    """

    name: str
    definition: str
    kind: str
    is_symbol: bool
    indexed: bool


class TableSchema(NamedTuple):
    """A parsed CREATE TABLE: ordered columns plus designated-timestamp info.

    ``columns`` is in declared order and INCLUDES the designated timestamp column.
    ``partition_by`` is the unit from the DDL, or None if the statement omitted it.
    """

    name: str
    columns: List[ColumnDef]
    timestamp_col: str
    timestamp_type: str
    partition_by: Optional[str]

    def symbol_columns(self) -> List[str]:
        """Tag (SYMBOL) column names, in declared order, excluding the timestamp."""
        return [c.name for c in self.columns if c.is_symbol and c.name != self.timestamp_col]

    def field_columns(self) -> List[str]:
        """Non-tag, non-timestamp column names (the pivoted measurement fields)."""
        return [
            c.name
            for c in self.columns
            if not c.is_symbol and c.name != self.timestamp_col
        ]


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
    _exec_ddl(base_url, auth, ddl, table, timeout)
    cols = ", ".join(
        "%s%s" % (n, "(cap %d)" % c if c else "") for n, c in index_specs
    )
    log.info("ensured table %s with indexed columns: %s", table, cols or "(none)")
    return ddl


def _exec_ddl(
    base_url: str,
    auth: Optional[str],
    ddl: str,
    table: str,
    timeout: float = 60.0,
) -> None:
    """POST a DDL statement to QuestDB's ``/exec``; raise SystemExit on failure.

    Aborting loudly on an HTTP/transport error keeps a misconfigured pre-create
    from silently proceeding into a missing or mismatched table.
    """
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


def ensure_full_table(
    base_url: str,
    auth: Optional[str],
    schema: TableSchema,
    timestamp_type: Optional[str] = None,
    partition_by: Optional[str] = None,
    dedup: bool = True,
    wal: bool = True,
    timeout: float = 60.0,
) -> str:
    """Build and execute a complete CREATE TABLE for ``schema`` (idempotent).

    Returns the DDL run. Used by the COPY path, which needs the full column list
    to exist before the import (COPY validates the CSV header and cannot auto-add
    columns). ``CREATE TABLE IF NOT EXISTS`` makes re-runs a no-op and leaves an
    existing table untouched.
    """
    ddl = build_full_create_table_ddl(schema, timestamp_type, partition_by, dedup, wal)
    _exec_ddl(base_url, auth, ddl, schema.name, timeout)
    log.info(
        "ensured full table %s (%d columns, %d tag(s))",
        schema.name,
        len(schema.columns),
        len(schema.symbol_columns()),
    )
    return ddl


def parse_schema_tables(sql_text: str) -> Dict[str, TableSchema]:
    """Parse ``CREATE TABLE`` statements into ``{table: TableSchema}``.

    Preserves column order, the verbatim type/modifier text per column, the
    designated timestamp column and its type, and the ``PARTITION BY`` unit (None
    if absent). Line (``--``) comments are stripped first. The body is split on
    commas, so a column type carrying a comma (e.g. ``DECIMAL(10,2)``) is not
    supported; this targets the simple ``name TYPE [INDEX ...]`` column lists the
    migration emits.
    """
    text = re.sub(r"--[^\n]*", "", sql_text)
    out: Dict[str, TableSchema] = {}
    for match in _CREATE_TABLE_RE.finditer(text):
        table, body, ts_col = match.group(1), match.group(2), match.group(3)
        partition_by = match.group(4).upper() if match.group(4) else None
        columns: List[ColumnDef] = []
        ts_type = "TIMESTAMP"
        for coldef in body.split(","):
            coldef = coldef.strip()
            tokens = coldef.split()
            if len(tokens) < 2:
                continue
            name, type_name = tokens[0], tokens[1].upper()
            definition = coldef[len(name):].strip()
            upper_tokens = [t.upper() for t in tokens[1:]]
            is_symbol = type_name == "SYMBOL"
            indexed = "INDEX" in upper_tokens
            columns.append(
                ColumnDef(
                    name=name,
                    definition=definition,
                    kind=_TYPE_KIND.get(type_name, "str"),
                    is_symbol=is_symbol,
                    indexed=indexed,
                )
            )
            if name == ts_col:
                ts_type = type_name
        out[table] = TableSchema(
            name=table,
            columns=columns,
            timestamp_col=ts_col,
            timestamp_type=ts_type,
            partition_by=partition_by,
        )
    return out


def parse_schema_columns(sql_text: str) -> "dict[str, dict[str, str]]":
    """Parse ``CREATE TABLE`` statements into ``{table: {column: kind}}``.

    ``kind`` is one of ``bool`` / ``int`` / ``float`` / ``str`` (see
    :data:`_TYPE_KIND`). The designated timestamp column is excluded -- in line
    protocol the timestamp is the trailing element, never a field token, so it is
    not part of the field allow-list. Thin view over :func:`parse_schema_tables`.
    """
    out: "dict[str, dict[str, str]]" = {}
    for table, schema in parse_schema_tables(sql_text).items():
        out[table] = {
            c.name: c.kind
            for c in schema.columns
            if c.name != schema.timestamp_col
        }
    return out


def build_full_create_table_ddl(
    schema: TableSchema,
    timestamp_type: Optional[str] = None,
    partition_by: Optional[str] = None,
    dedup: bool = True,
    wal: bool = True,
) -> str:
    """Build a complete idempotent CREATE TABLE re-emitting every parsed column.

    Unlike :func:`build_create_table_ddl` (which declares only the timestamp and
    indexed columns and lets the ILP feed auto-create the rest), this emits the
    FULL column list so QuestDB's COPY -- which validates the CSV header against an
    existing table and cannot auto-add columns mid-import -- has a complete target.

    ``timestamp_type`` overrides the designated timestamp's declared type (e.g.
    force ``TIMESTAMP_NS``); ``partition_by`` overrides the unit. When ``dedup`` is
    set and there is at least one tag column, appends
    ``DEDUP UPSERT KEYS(<timestamp>, <tags...>)`` so a resumed/re-run import is
    idempotent on the (timestamp, tag-set) key. Raises :class:`IndexSpecError` on
    an unknown timestamp type, an empty table, or a missing PARTITION BY.
    """
    if not schema.name or not schema.name.strip():
        raise IndexSpecError("table name is required")
    ts_type = (timestamp_type or schema.timestamp_type).strip().upper()
    if ts_type not in _VALID_TIMESTAMP_TYPES:
        raise IndexSpecError(
            "timestamp_type must be one of %s, got %r"
            % (", ".join(_VALID_TIMESTAMP_TYPES), ts_type)
        )
    part = (partition_by or schema.partition_by or "").strip().upper()
    if not part:
        raise IndexSpecError(
            "PARTITION BY is required (none in DDL for %r, none overridden)"
            % schema.name
        )
    col_lines: List[str] = []
    for col in schema.columns:
        if col.name == schema.timestamp_col:
            col_lines.append("    %s %s" % (col.name, ts_type))
        else:
            col_lines.append("    %s %s" % (col.name, col.definition))
    wal_clause = " WAL" if wal else ""
    ddl = (
        "CREATE TABLE IF NOT EXISTS '%s' (\n%s\n) timestamp(%s) PARTITION BY %s%s"
        % (schema.name, ",\n".join(col_lines), schema.timestamp_col, part, wal_clause)
    )
    if dedup:
        keys = [schema.timestamp_col] + schema.symbol_columns()
        ddl += "\nDEDUP UPSERT KEYS(%s)" % ", ".join(keys)
    return ddl


def fetch_table_schema(
    base_url: str,
    auth: Optional[str],
    table: str,
    timeout: float = 60.0,
) -> Optional[TableSchema]:
    """Read an existing table's schema from QuestDB via ``SHOW COLUMNS``.

    Returns a :class:`TableSchema` reflecting the live table -- column order,
    types, which column is the designated timestamp, and which columns are SYMBOL
    (tags) -- so the CSV pivot can build a header that COPY will accept WITHOUT a
    schema file. Returns None when the table does not exist. Raises SystemExit on
    any other HTTP/transport error so a real failure is not silently treated as
    "no such table".

    ``partition_by`` is left None (``SHOW COLUMNS`` does not report it); callers
    that only need the column set for an existing table do not pre-create it, so
    the partitioning is irrelevant here.
    """
    sql = "SHOW COLUMNS FROM '%s'" % table
    url = base_url.rstrip("/") + "/exec?" + urllib.parse.urlencode({"query": sql})
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", auth)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            doc = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        if "does not exist" in detail or "table does not exist" in detail.lower():
            return None
        raise SystemExit(
            "QuestDB SHOW COLUMNS failed for %r (HTTP %d): %s"
            % (table, exc.code, detail.strip())
        ) from exc
    except urllib.error.URLError as exc:
        raise SystemExit(
            "QuestDB /exec unreachable for SHOW COLUMNS: %s" % exc.reason
        ) from exc

    idx = {c["name"]: i for i, c in enumerate(doc.get("columns", []))}
    name_i = idx.get("column")
    type_i = idx.get("type")
    indexed_i = idx.get("indexed")
    designated_i = idx.get("designated")
    if name_i is None or type_i is None:
        raise SystemExit(
            "QuestDB SHOW COLUMNS for %r returned unexpected columns: %s"
            % (table, list(idx))
        )
    columns: List[ColumnDef] = []
    ts_col = "timestamp"
    ts_type = "TIMESTAMP"
    for row in doc.get("dataset", []):
        name = row[name_i]
        type_name = str(row[type_i]).upper()
        is_symbol = type_name == "SYMBOL"
        indexed = bool(row[indexed_i]) if indexed_i is not None else False
        definition = "SYMBOL" if is_symbol else type_name
        if indexed:
            definition += " INDEX"
        columns.append(
            ColumnDef(
                name=name,
                definition=definition,
                kind=_TYPE_KIND.get(type_name, "str"),
                is_symbol=is_symbol,
                indexed=indexed,
            )
        )
        if designated_i is not None and bool(row[designated_i]):
            ts_col = name
            ts_type = type_name
    if not columns:
        return None
    return TableSchema(
        name=table,
        columns=columns,
        timestamp_col=ts_col,
        timestamp_type=ts_type,
        partition_by=None,
    )
