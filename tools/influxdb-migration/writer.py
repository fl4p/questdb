"""QuestDB writer over InfluxDB Line Protocol (ILP).

Wraps the official ``questdb`` Python ``Sender`` so all ILP escaping, batching
and retry logic is handled by the client rather than hand-rolled. Tags are sent
as SYMBOL columns and fields as typed columns, which is exactly the layout the
fork's InfluxQL /query endpoint relies on (SYMBOL == tag, everything else ==
field, the row timestamp == designated timestamp).

In ``--dry-run`` mode the writer counts rows and reports planned tables without
opening a connection or sending anything.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

from model import FieldType, MigrationStats, Row, TableSchema

log = logging.getLogger("influx_migrate.writer")


class QuestDBWriter:
    """Writes :class:`Row` objects into QuestDB via ILP, or counts them (dry run)."""

    def __init__(self, conf: str, dry_run: bool = False):
        self._conf = conf
        self._dry_run = dry_run
        self._sender = None
        self._stats: Dict[str, MigrationStats] = {}
        if not dry_run:
            try:
                from questdb.ingress import Sender  # lazy import
            except ImportError as exc:  # pragma: no cover - dependency guidance
                raise SystemExit(
                    "Writing needs the 'questdb' package: pip install questdb"
                ) from exc
            self._Sender = Sender
            self._sender = Sender.from_conf(conf)
            self._sender.establish()

    def stats(self) -> Dict[str, MigrationStats]:
        return self._stats

    def write(self, row: Row, schema: TableSchema) -> None:
        """Write (or count) a single row.

        ``schema.field_types`` decides how each field is coerced. Fields whose
        value fails coercion are dropped with a warning rather than aborting the
        whole migration.
        """
        st = self._stats.setdefault(row.table, MigrationStats())

        symbols = {k: v for k, v in row.tags.items() if v != ""}
        columns = {}
        for name, value in row.fields.items():
            coerced = _coerce(value, schema.field_types.get(name))
            if coerced is None:
                st.fields_skipped_null += 1
                continue
            columns[name] = coerced

        st.tags_skipped_empty += len(row.tags) - len(symbols)

        if columns or symbols:
            st.rows += 1
            if not self._dry_run:
                from questdb.ingress import TimestampNanos

                self._sender.row(
                    row.table,
                    symbols=symbols or None,
                    columns=columns or None,
                    at=TimestampNanos(row.ts_ns),
                )

    def flush(self) -> None:
        if self._sender is not None:
            self._sender.flush()

    def close(self) -> None:
        if self._sender is not None:
            try:
                self._sender.flush()
            finally:
                self._sender.close()
                self._sender = None

    def __enter__(self) -> "QuestDBWriter":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _coerce(value: object, ftype: Optional[FieldType]):
    """Coerce a source value to the QuestDB-facing type, or None to skip it.

    A None return means "do not write this column" — either the value is null or
    it cannot be represented as the declared type. Booleans are checked before
    int because ``bool`` is a subclass of ``int`` in Python.
    """
    if value is None:
        return None
    try:
        if ftype == FieldType.BOOLEAN:
            return bool(value)
        if ftype == FieldType.INTEGER:
            return int(value)
        if ftype == FieldType.FLOAT:
            return float(value)
        if ftype == FieldType.STRING:
            return str(value)
    except (TypeError, ValueError):
        log.warning("could not coerce %r to %s; skipping field", value, ftype)
        return None

    # No declared type (e.g. a v2 field discovered mid-stream): pass through the
    # native Python type, which the Sender maps to the natural ILP type.
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, str)):
        return value
    return str(value)
