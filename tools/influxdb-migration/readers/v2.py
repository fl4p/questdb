"""InfluxDB 2.x reader (Flux over the /api/v2 API).

Uses the ``influxdb-client`` library, imported lazily. Buckets play the role of
v1 databases (one bucket -> one ``<bucket>_`` table prefix). Schema comes from
the Flux ``schema.tagKeys`` / ``schema.fieldKeys`` helpers; data is read in
bounded time windows (one request each, retried on transient drops) so a large
bucket never materializes in full -- only one window at a time; principals are
derived from API tokens (authorizations), since v2 has no per-database
READ/WRITE user model.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Iterator, List, Optional

from model import (
    Access,
    FieldType,
    Grant,
    Principal,
    Row,
    TableSchema,
)
from readers.base import InfluxReader

log = logging.getLogger("influx_migrate.v2")

# Flux _value Python types -> our FieldType.
_PY_TYPE_MAP = {
    float: FieldType.FLOAT,
    int: FieldType.INTEGER,
    bool: FieldType.BOOLEAN,
    str: FieldType.STRING,
}

# Columns the pivoted Flux table carries that are neither tag nor field.
_RESERVED = {"_time", "_start", "_stop", "_measurement", "result", "table"}


class V2Reader(InfluxReader):
    """Reads one InfluxDB 2.x server for a single organization."""

    def __init__(
        self,
        url: str,
        token: str,
        org: str,
        window: Optional[timedelta] = None,
        timeout_ms: int = 300_000,
        pool_maxsize: int = 16,
    ):
        try:
            from influxdb_client import InfluxDBClient  # lazy import
        except ImportError as exc:  # pragma: no cover - dependency guidance
            raise SystemExit(
                "InfluxDB v2 source needs the 'influxdb-client' package: "
                "pip install influxdb-client"
            ) from exc

        self._org = org
        # Read the data in time windows: a single unbounded range(start: 0) query
        # over a large bucket makes InfluxDB compute the whole pivot before
        # streaming a byte, which blows the client read timeout. Each window is
        # streamed (bounded memory) AND time-bounded (bounded server work).
        self._window = window or timedelta(hours=1)
        # connection_pool_maxsize must cover the parallel worker count: worker
        # threads issue window queries concurrently against this shared client,
        # so a small pool would serialize them on the HTTP connections.
        self._client = InfluxDBClient(
            url=url,
            token=token,
            org=org,
            timeout=timeout_ms,
            connection_pool_maxsize=max(pool_maxsize, 1),
        )
        self._query_api = self._client.query_api()

    def scopes(self) -> List[str]:
        buckets_api = self._client.buckets_api()
        buckets = buckets_api.find_buckets().buckets or []
        # Skip the internal monitoring buckets InfluxDB ships with.
        return [b.name for b in buckets if not b.name.startswith("_")]

    def measurements(self, scope: str) -> List[str]:
        # start: 0 covers all of time. The schema.* helpers otherwise default to
        # the last 30 days, which silently misses older data (and would report
        # no measurements/tags/fields for a backfill).
        flux = (
            f'import "influxdata/influxdb/schema"\n'
            f'schema.measurements(bucket: "{_esc(scope)}", start: 0)'
        )
        return [r["_value"] for r in self._flat_records(flux)]

    def schema(self, scope: str, measurement: str) -> TableSchema:
        ts = TableSchema(table=measurement)
        tag_flux = (
            f'import "influxdata/influxdb/schema"\n'
            f'schema.measurementTagKeys(bucket: "{_esc(scope)}", '
            f'measurement: "{_esc(measurement)}", start: 0)'
        )
        ts.tag_keys = [
            r["_value"]
            for r in self._flat_records(tag_flux)
            if not r["_value"].startswith("_")
        ]
        field_flux = (
            f'import "influxdata/influxdb/schema"\n'
            f'schema.measurementFieldKeys(bucket: "{_esc(scope)}", '
            f'measurement: "{_esc(measurement)}", start: 0)'
        )
        for r in self._flat_records(field_flux):
            # Field types are inferred per row from the actual _value during the
            # data pass; default to FLOAT here and refine as values arrive.
            ts.field_types[r["_value"]] = FieldType.FLOAT
        return ts

    def plan_windows(self, scope: str, measurement: str) -> List[object]:
        # Split the measurement's real data span into half-open [start, stop)
        # time windows -- the unit of parallelism. InfluxDB stores points in
        # time-partitioned shards, so windowed queries hit different shards and
        # the server serves them concurrently. Shared boundaries (next start ==
        # this stop) place every point in exactly one window: no loss, no dup.
        oldest = self._oldest(scope, measurement)
        if oldest is None:
            return []
        # Bound to [oldest, newest], not now(): backfilled data can sit years in
        # the past, and stepping to now() would issue tens of thousands of empty
        # windows. +1us makes the final window's exclusive stop include newest.
        newest = self._newest(scope, measurement) or oldest
        end = newest + timedelta(microseconds=1)
        windows: List[object] = []
        start = oldest
        while start < end:
            stop = min(start + self._window, end)
            windows.append((start, stop))
            start = stop
        return windows

    def rows_window(
        self, scope: str, measurement: str, schema: TableSchema, window: object
    ) -> Iterator[Row]:
        # Read one [start, stop) window in full (one request, retried on transient
        # drops) so memory is bounded by the window size, not the whole bucket.
        # pivot turns the long _field/_value form into one row per timestamp+tagset.
        start, stop = window  # type: ignore[misc]
        tag_set = set(schema.tag_keys)
        flux = (
            f'from(bucket: "{_esc(scope)}")\n'
            f"  |> range(start: {_rfc3339(start)}, stop: {_rfc3339(stop)})\n"
            f'  |> filter(fn: (r) => r._measurement == "{_esc(measurement)}")\n'
            f'  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
        )
        for rec in self._flat_records(flux):
            ts_ns = _ns(rec.get("_time"))
            if ts_ns is None:
                continue
            tags = {}
            fields = {}
            for key, value in rec.items():
                if key in _RESERVED or key.startswith("_") or value is None:
                    continue
                if key in tag_set:
                    sval = str(value)
                    if sval == "":
                        continue
                    tags[key] = sval
                    continue
                fields[key] = value
                ft = _PY_TYPE_MAP.get(type(value))
                if ft is not None:
                    schema.field_types[key] = ft
            yield Row(table=measurement, tags=tags, fields=fields, ts_ns=ts_ns)

    def rows(self, scope: str, measurement: str, schema: TableSchema) -> Iterator[Row]:
        # Sequential convenience kept for direct callers/tests: read every window
        # in order. The orchestrator instead dispatches plan_windows() units
        # across threads via rows_window().
        for window in self.plan_windows(scope, measurement):
            yield from self.rows_window(scope, measurement, schema, window)

    def _oldest(self, scope: str, measurement: str) -> Optional[datetime]:
        # Earliest timestamp, so windowing starts at real data rather than
        # scanning from epoch. first() returns one row PER SERIES, which on a
        # high-cardinality measurement is tens of thousands of rows -- dragging
        # all of them to the client just to take a min stalls the run. So we
        # collapse server-side: first() pushes down per series, then group() +
        # min(_time) reduce to a SINGLE global-earliest row before it leaves
        # InfluxDB. Streamed result is one row regardless of cardinality.
        flux = (
            f'from(bucket: "{_esc(scope)}")\n'
            f"  |> range(start: 0)\n"
            f'  |> filter(fn: (r) => r._measurement == "{_esc(measurement)}")\n'
            f"  |> first()\n"
            f'  |> keep(columns: ["_time"])\n'
            f"  |> group()\n"
            f'  |> min(column: "_time")'
        )
        earliest: Optional[datetime] = None
        for rec in self._flat_records(flux):
            t = _to_dt(rec.get("_time"))
            if t is not None and (earliest is None or t < earliest):
                earliest = t
        return earliest

    def _newest(self, scope: str, measurement: str) -> Optional[datetime]:
        # Latest timestamp, to bound windowing to the real data span. Mirrors
        # _oldest: last() pushes down one row per series, then group() +
        # max(_time) collapse to a single global-latest row server-side, so the
        # client never materializes the full per-series fan-out.
        flux = (
            f'from(bucket: "{_esc(scope)}")\n'
            f"  |> range(start: 0)\n"
            f'  |> filter(fn: (r) => r._measurement == "{_esc(measurement)}")\n'
            f"  |> last()\n"
            f'  |> keep(columns: ["_time"])\n'
            f"  |> group()\n"
            f'  |> max(column: "_time")'
        )
        latest: Optional[datetime] = None
        for rec in self._flat_records(flux):
            t = _to_dt(rec.get("_time"))
            if t is not None and (latest is None or t > latest):
                latest = t
        return latest

    def principals(self) -> List[Principal]:
        principals: List[Principal] = []
        try:
            auths = self._client.authorizations_api().find_authorizations()
        except Exception as exc:  # noqa: BLE001 - needs an operator/admin token
            log.warning("cannot read authorizations: %s; skipping ACL", exc)
            return principals
        bucket_id_to_name = self._bucket_id_index()
        for auth in auths or []:
            # Name the acl.conf user after the token's DESCRIPTION: it is unique
            # per token, whereas auth.user is the owner and is shared across all
            # of that user's tokens (so naming by user collides). Fall back to a
            # token-id-based name when a token has no description.
            desc = (getattr(auth, "description", None) or "").strip()
            name = desc or ("token-" + str(getattr(auth, "id", "") or "unknown")[:12])
            is_admin = _is_all_access(auth)
            principal = Principal(name=name, is_admin=is_admin)
            if not is_admin:
                principal.grants = self._grants_from_permissions(
                    auth, bucket_id_to_name
                )
            principals.append(principal)
        return principals

    def _grants_from_permissions(self, auth, bucket_id_to_name) -> List[Grant]:
        # Collapse read/write permissions per bucket into one ro/rw grant.
        access_by_bucket = {}
        for perm in getattr(auth, "permissions", None) or []:
            resource = getattr(perm, "resource", None)
            action = (getattr(perm, "action", "") or "").lower()
            if resource is None or getattr(resource, "type", "") != "buckets":
                continue
            bucket = getattr(resource, "name", None)
            if bucket is None:
                bid = getattr(resource, "id", None)
                bucket = bucket_id_to_name.get(bid)
            if not bucket or bucket.startswith("_"):
                continue
            current = access_by_bucket.get(bucket, Access.RO)
            if action == "write":
                access_by_bucket[bucket] = Access.RW
            else:
                access_by_bucket.setdefault(bucket, current)
        return [Grant(scope=b, access=a) for b, a in access_by_bucket.items()]

    def _bucket_id_index(self):
        try:
            buckets = self._client.buckets_api().find_buckets().buckets or []
            return {b.id: b.name for b in buckets}
        except Exception:  # noqa: BLE001
            return {}

    def _flat_records(self, flux: str, attempts: int = 4):
        # query() returns the whole (window-bounded) result in one request and
        # materializes it, so a retry after a transient connection drop
        # (RemoteDisconnected / read timeout from a stale pooled connection
        # between windows) re-reads the window cleanly -- no half-written window,
        # no duplicates. Memory is bounded by the window size (lower
        # --v2-window-minutes for very dense data).
        import time as _time

        last_exc = None
        for attempt in range(attempts):
            try:
                tables = self._query_api.query(flux, org=self._org)
                return [record.values for table in tables for record in table.records]
            except Exception as exc:  # noqa: BLE001 - transient HTTP/connection errors
                last_exc = exc
                log.warning(
                    "v2 query attempt %d/%d failed: %s", attempt + 1, attempts, exc
                )
                _time.sleep(min(2.0, 0.5 * (attempt + 1)))
        raise last_exc

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


def _is_all_access(auth) -> bool:
    """True if a token has org-wide all-access (treated as admin/no-prefix)."""
    perms = getattr(auth, "permissions", None) or []
    for perm in perms:
        resource = getattr(perm, "resource", None)
        # An all-buckets permission has no specific name/id.
        if (
            resource is not None
            and getattr(resource, "type", "") == "buckets"
            and getattr(resource, "name", None) is None
            and getattr(resource, "id", None) is None
        ):
            return True
    return False


def _esc(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _rfc3339(dt: datetime) -> str:
    # Microsecond precision so a sub-second window bound is not truncated. The
    # bound only filters; half-open windows with shared edges keep every point.
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _to_dt(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return None


def _ns(value) -> Optional[int]:
    dt = _to_dt(value)
    if dt is None:
        return None
    return int(dt.timestamp() * 1_000_000_000)
