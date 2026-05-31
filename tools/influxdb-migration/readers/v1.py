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
    ):
        try:
            from influxdb import InfluxDBClient  # lazy import
        except ImportError as exc:  # pragma: no cover - dependency guidance
            raise SystemExit(
                "InfluxDB v1 source needs the 'influxdb' package: pip install influxdb"
            ) from exc

        self._page_size = max(1, page_size)
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

    def rows(self, scope: str, measurement: str, schema: TableSchema) -> Iterator[Row]:
        self._client.switch_database(scope)
        tag_set = set(schema.tag_keys)
        field_set = set(schema.field_types)
        # Keyset-paginate by time to bound memory. chunked mode is unusable (it
        # raises msgpack ExtraData in influxdb-python 5.x), and a single
        # SELECT * would materialize the whole measurement. We page with
        # ORDER BY time ASC LIMIT <page>, advancing a "time >= cursor" bound.
        # Many series can share one timestamp, so the LIMIT may cut a timestamp
        # in half: we hold back every row at the page's max timestamp and
        # re-query from that timestamp, which re-reads it in full. SELECT *
        # returns time + tags + fields per row.
        page = self._page_size
        cursor = None  # ns; None = from the beginning
        while True:
            where = "" if cursor is None else f" WHERE time >= {cursor}"
            query = (
                f"SELECT * FROM {_ident(measurement)}{where} "
                f"ORDER BY time ASC LIMIT {page}"
            )
            points = list(self._client.query(query, epoch="ns").get_points())
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
                # then step strictly past it.
                exact = self._client.query(
                    f"SELECT * FROM {_ident(measurement)} WHERE time = {max_t}",
                    epoch="ns",
                ).get_points()
                for point in exact:
                    row = self._point_to_row(point, measurement, tag_set, field_set)
                    if row is not None:
                        yield row
                cursor = max_t + 1

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
