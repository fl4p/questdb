"""InfluxDB 1.x reader (InfluxQL over the HTTP /query API).

Uses the ``influxdb`` client library, imported lazily so v2-only users do not
need it installed. Schema comes from ``SHOW TAG KEYS`` / ``SHOW FIELD KEYS``;
data is read with ``SELECT *`` keyset-paginated by time (``LIMIT`` + a
``time >= cursor`` bound) at nanosecond epoch precision, which bounds memory on
large measurements (chunked mode is avoided -- it triggers a msgpack bug in
influxdb-python 5.x); principals come from ``SHOW USERS`` / ``SHOW GRANTS``.
"""

from __future__ import annotations

import logging
from datetime import timedelta
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

log = logging.getLogger("influx_migrate.v1")

# InfluxDB v1 SHOW FIELD KEYS reports these field type names.
_FIELD_TYPE_MAP = {
    "float": FieldType.FLOAT,
    "integer": FieldType.INTEGER,
    "string": FieldType.STRING,
    "boolean": FieldType.BOOLEAN,
}


class V1Reader(InfluxReader):
    """Reads one InfluxDB 1.x server."""

    def __init__(
        self,
        url: str,
        username: str = "",
        password: str = "",
        timeout: int = 60,
        page_size: int = 50_000,
        window: Optional[timedelta] = None,
        pool_maxsize: int = 16,
    ):
        try:
            from influxdb import InfluxDBClient  # lazy import
        except ImportError as exc:  # pragma: no cover - dependency guidance
            raise SystemExit(
                "InfluxDB v1 source needs the 'influxdb' package: pip install influxdb"
            ) from exc

        self._page_size = max(1, page_size)
        # Window size for plan_windows(): a measurement's [oldest, newest] span is
        # sliced into windows of this many nanoseconds, each an independent read
        # unit the orchestrator dispatches to its own worker thread. Default 6h.
        self._window_ns = _timedelta_to_ns(window or timedelta(hours=6))
        host, port, ssl = _parse_url(url)
        self._client = InfluxDBClient(
            host=host,
            port=port,
            username=username or None,
            password=password or None,
            ssl=ssl,
            verify_ssl=ssl,
            timeout=timeout,
        )
        # influxdb-python wraps a requests.Session whose default connection pool
        # (pool_maxsize ~10) is too small once many worker threads issue window
        # queries concurrently against this one shared client: requests would
        # otherwise warn ("Connection pool is full") and serialize. Enlarge the
        # pool to cover the worker count. Guard with getattr so a client layout
        # without an exposed _session does not crash the migration.
        session = getattr(self._client, "_session", None)
        if session is not None:
            try:
                from requests.adapters import HTTPAdapter

                size = max(pool_maxsize, 1)
                adapter = HTTPAdapter(pool_connections=size, pool_maxsize=size)
                session.mount("http://", adapter)
                session.mount("https://", adapter)
            except Exception:  # noqa: BLE001 - pool tuning is best-effort
                pass

    def scopes(self) -> List[str]:
        # get_list_database() requires admin; fall back to SHOW DATABASES.
        try:
            return [d["name"] for d in self._client.get_list_database()]
        except Exception:  # noqa: BLE001 - non-admin token, degrade gracefully
            rs = self._client.query("SHOW DATABASES")
            return [p["name"] for p in rs.get_points()]

    def measurements(self, scope: str) -> List[str]:
        self._client.switch_database(scope)
        rs = self._client.query("SHOW MEASUREMENTS")
        return [p["name"] for p in rs.get_points()]

    def schema(self, scope: str, measurement: str) -> TableSchema:
        self._client.switch_database(scope)
        ts = TableSchema(table=measurement)
        tag_rs = self._client.query(f'SHOW TAG KEYS FROM {_ident(measurement)}')
        ts.tag_keys = [p["tagKey"] for p in tag_rs.get_points()]
        field_rs = self._client.query(f'SHOW FIELD KEYS FROM {_ident(measurement)}')
        for p in field_rs.get_points():
            name = p["fieldKey"]
            raw = p.get("fieldType", "float")
            if raw not in _FIELD_TYPE_MAP:
                log.warning(
                    "measurement %s field %s has unknown type %r; treating as string",
                    measurement,
                    name,
                    raw,
                )
            ts.field_types[name] = _FIELD_TYPE_MAP.get(raw, FieldType.STRING)
        return ts

    def plan_windows(self, scope: str, measurement: str) -> List[object]:
        # Split the measurement's real data span into half-open [start_ns, stop_ns)
        # time windows -- the unit of parallelism. InfluxDB stores points in
        # time-partitioned shards, so windowed queries hit different shards the
        # server can serve concurrently. Shared boundaries (next start == this
        # stop) place every point in exactly one window: no loss, no dup.
        oldest = self._extreme_ts(scope, measurement, ascending=True)
        if oldest is None:
            return []  # empty measurement: nothing to read
        # Bound to [oldest, newest], not now(): backfilled data can sit years in
        # the past, and stepping to now() would issue tens of thousands of empty
        # windows. +1ns makes the final window's exclusive stop include newest.
        newest = self._extreme_ts(scope, measurement, ascending=False)
        if newest is None:
            newest = oldest
        end = newest + 1
        windows: List[object] = []
        start = oldest
        while start < end:
            stop = min(start + self._window_ns, end)
            windows.append((start, stop))
            start = stop
        return windows

    def rows_window(
        self, scope: str, measurement: str, schema: TableSchema, window: object
    ) -> Iterator[Row]:
        # Read ONE [start_ns, stop_ns) window with the same keyset pagination
        # rows() always used, just additionally bounded by the window edges. The
        # data path runs under the thread pool, so it must NOT mutate shared
        # client state via switch_database (that races across workers); it passes
        # database=scope explicitly on every query instead.
        start_ns, stop_ns = window  # type: ignore[misc]
        tag_set = set(schema.tag_keys)
        field_set = set(schema.field_types)
        # Keyset-paginate by time to bound memory. chunked mode is unusable (it
        # raises msgpack ExtraData in influxdb-python 5.x), and a single
        # SELECT * would materialize the whole window. We page with
        # ORDER BY time ASC LIMIT <page>, advancing a "time >= cursor" lower
        # bound while the window's "time < stop_ns" upper bound stays fixed.
        # Many series can share one timestamp, so the LIMIT may cut a timestamp
        # in half: we hold back every row at the page's max timestamp and
        # re-query from that timestamp, which re-reads it in full. SELECT *
        # returns time + tags + fields per row.
        page = self._page_size
        cursor = start_ns  # ns; advancing lower bound, starts at the window edge
        while True:
            query = (
                f"SELECT * FROM {_ident(measurement)} "
                f"WHERE time >= {cursor} AND time < {stop_ns} "
                f"ORDER BY time ASC LIMIT {page}"
            )
            points = list(
                self._client.query(query, epoch="ns", database=scope).get_points()
            )
            if not points:
                return
            if len(points) < page:
                # Final (partial) page: nothing held back, emit everything.
                for point in points:
                    row = self._point_to_row(point, measurement, tag_set, field_set)
                    if row is not None:
                        yield row
                return
            max_t = points[-1]["time"]
            if any(p.get("time", max_t) < max_t for p in points):
                # Hold back the boundary timestamp; re-read it next round.
                for point in points:
                    if point.get("time") == max_t:
                        continue
                    row = self._point_to_row(point, measurement, tag_set, field_set)
                    if row is not None:
                        yield row
                cursor = max_t
            else:
                # The whole (full) page sits at one timestamp, so that timestamp
                # has at least `page` rows and may have MORE than the page
                # captured. Re-read the timestamp in full so none are skipped,
                # then step strictly past it. The exact-timestamp refetch stays
                # inside the window because max_t < stop_ns holds (it came from a
                # row that satisfied the window's upper bound).
                exact = self._client.query(
                    f"SELECT * FROM {_ident(measurement)} WHERE time = {max_t}",
                    epoch="ns",
                    database=scope,
                ).get_points()
                for point in exact:
                    row = self._point_to_row(point, measurement, tag_set, field_set)
                    if row is not None:
                        yield row
                cursor = max_t + 1

    def rows(self, scope: str, measurement: str, schema: TableSchema) -> Iterator[Row]:
        # Sequential convenience kept for direct callers/tests: read every window
        # in order. The orchestrator instead dispatches plan_windows() units
        # across threads via rows_window(). Identical rows, same per-measurement
        # order, no loss/dup -- windows partition time and each is paged in order.
        for window in self.plan_windows(scope, measurement):
            yield from self.rows_window(scope, measurement, schema, window)

    def _extreme_ts(
        self, scope: str, measurement: str, ascending: bool
    ) -> Optional[int]:
        # Earliest (ascending) or latest timestamp of the measurement, in ns
        # epoch, so windowing spans the real data rather than scanning from epoch
        # to now(). Reads one row only (LIMIT 1). Uses database=scope so it is
        # also safe to call concurrently. Returns None when the measurement holds
        # no data.
        order = "ASC" if ascending else "DESC"
        query = (
            f"SELECT * FROM {_ident(measurement)} "
            f"ORDER BY time {order} LIMIT 1"
        )
        points = list(
            self._client.query(query, epoch="ns", database=scope).get_points()
        )
        for point in points:
            ts = point.get("time")
            if ts is not None:
                return int(ts)
        return None

    def _point_to_row(self, point, measurement, tag_set, field_set) -> "Optional[Row]":
        ts_ns = point.get("time")
        if ts_ns is None:
            return None  # a point with no timestamp cannot be placed
        tags = {}
        fields = {}
        for key, value in point.items():
            if key == "time" or value is None:
                continue  # skip the timestamp col and sparse/absent values
            if key in tag_set:
                sval = str(value)
                if sval == "":
                    continue  # empty tag -> no SYMBOL
                tags[key] = sval
            elif key in field_set:
                fields[key] = value
            else:
                # Column neither in tag nor field schema (e.g. a tag with no
                # values at schema time). Default to field.
                fields[key] = value
        return Row(table=measurement, tags=tags, fields=fields, ts_ns=int(ts_ns))

    def principals(self) -> List[Principal]:
        principals: List[Principal] = []
        try:
            users = self._client.get_list_users()
        except Exception as exc:  # noqa: BLE001 - needs admin privileges
            log.warning("cannot read users (admin required): %s; skipping ACL", exc)
            return principals
        for user in users:
            name = user.get("user")
            is_admin = bool(user.get("admin", False))
            principal = Principal(name=name, is_admin=is_admin)
            if not is_admin:
                principal.grants = self._grants_for(name)
            principals.append(principal)
        return principals

    def _grants_for(self, name: str) -> List[Grant]:
        grants: List[Grant] = []
        try:
            rs = self._client.query(f'SHOW GRANTS FOR {_ident(name)}')
        except Exception as exc:  # noqa: BLE001
            log.warning("cannot read grants for %s: %s", name, exc)
            return grants
        for p in rs.get_points():
            db = p.get("database")
            privilege = (p.get("privilege") or "").upper()
            if not db:
                continue
            if privilege in ("ALL", "ALL PRIVILEGES", "WRITE"):
                grants.append(Grant(scope=db, access=Access.RW))
            elif privilege == "READ":
                grants.append(Grant(scope=db, access=Access.RO))
            elif privilege == "NO PRIVILEGES":
                continue
            else:
                log.warning("unknown privilege %r for %s on %s", privilege, name, db)
        return grants

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


def _timedelta_to_ns(delta: timedelta) -> int:
    """Convert a timedelta to integer nanoseconds (>= 1ns to avoid a stuck loop)."""
    ns = int(delta.total_seconds() * 1_000_000_000)
    return max(ns, 1)


def _ident(name: str) -> str:
    """Double-quote an InfluxQL identifier, escaping embedded quotes."""
    return '"' + name.replace('"', '\\"') + '"'


def _parse_url(url: str):
    """Split a URL into (host, port, ssl) for InfluxDBClient."""
    from urllib.parse import urlparse

    parsed = urlparse(url if "://" in url else f"http://{url}")
    ssl = parsed.scheme == "https"
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if ssl else 8086)
    return host, port, ssl
