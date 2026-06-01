"""Reader interface shared by the v1 and v2 InfluxDB readers.

A reader knows how to talk to one InfluxDB version. It enumerates scopes
(databases for v1, buckets for v2), exposes per-measurement schema, streams
rows in the common :class:`~model.Row` shape, and lists principals to replay
into acl.conf. Version-specific client libraries are imported lazily inside the
concrete readers so a single-version user only installs the dependency they
actually need.
"""

from __future__ import annotations

import abc
from typing import Iterable, Iterator, List

from model import Principal, Row, TableSchema


class InfluxReader(abc.ABC):
    """Abstract source. One instance is bound to one InfluxDB server."""

    @abc.abstractmethod
    def scopes(self) -> List[str]:
        """List the databases (v1) or buckets (v2) available to migrate."""

    @abc.abstractmethod
    def measurements(self, scope: str) -> List[str]:
        """List measurement names within a scope."""

    @abc.abstractmethod
    def schema(self, scope: str, measurement: str) -> TableSchema:
        """Return the tag keys and field types for one measurement.

        ``TableSchema.table`` is left as the bare measurement name here; the
        orchestrator applies the ``<db>_`` prefix policy when it knows whether a
        prefix is in effect.
        """

    @abc.abstractmethod
    def rows(self, scope: str, measurement: str, schema: TableSchema) -> Iterator[Row]:
        """Stream points of a measurement as :class:`Row` objects.

        Implementations MUST stream (chunked for v1, time-windowed for v2) so
        memory stays bounded on large series. ``Row.table`` is set to the bare
        measurement; the orchestrator rewrites it to the prefixed table name.
        """

    def plan_windows(self, scope: str, measurement: str) -> List[object]:
        """Split a measurement into independent read units for parallelism.

        Returns a list of opaque "window" tokens, each readable independently via
        :meth:`rows_window`. The orchestrator runs the units across a thread pool,
        so a reader that splits a measurement here (e.g. into time windows aligned
        with InfluxDB's time-sharded storage) gets intra-measurement parallelism.

        The default returns a single ``None`` token -- the whole measurement as
        one unit -- so a reader that does not override this still parallelizes
        *across* measurements. An empty list means the measurement has no data.
        """
        return [None]

    def rows_window(
        self, scope: str, measurement: str, schema: TableSchema, window: object
    ) -> Iterator[Row]:
        """Stream the rows of one :meth:`plan_windows` token.

        The default ignores the token and streams the whole measurement via
        :meth:`rows`, so a reader that does not override :meth:`plan_windows`
        still works under the parallel orchestrator (one unit per measurement).
        Overriders MUST keep windows non-overlapping and exhaustive so every
        point is read exactly once.
        """
        return self.rows(scope, measurement, schema)

    @abc.abstractmethod
    def principals(self) -> List[Principal]:
        """List users/tokens and their grants for acl.conf generation."""

    def close(self) -> None:
        """Release any underlying client resources. Overridden as needed."""


def iter_rows_with_table(
    rows: Iterable[Row], resolved_table: str
) -> Iterator[Row]:
    """Yield rows with ``table`` rewritten to the resolved (prefixed) name.

    Readers emit rows tagged with the bare measurement name; the orchestrator
    pipes them through here once it has computed the final QuestDB table name.
    """
    for row in rows:
        row.table = resolved_table
        yield row
