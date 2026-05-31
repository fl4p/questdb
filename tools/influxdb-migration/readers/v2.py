"""InfluxDB 2.x reader (Flux over the /api/v2 API).

Uses the ``influxdb-client`` library, imported lazily. Buckets play the role of
v1 databases (one bucket -> one ``<bucket>_`` table prefix). Schema comes from
the Flux ``schema.tagKeys`` / ``schema.fieldKeys`` helpers; data is read in
time windows so memory stays bounded; principals are derived from API tokens
(authorizations), since v2 has no per-database READ/WRITE user model.
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
        window: timedelta = timedelta(days=1),
        timeout_ms: int = 120_000,
    ):
        try:
            from influxdb_client import InfluxDBClient  # lazy import
        except ImportError as exc:  # pragma: no cover - dependency guidance
            raise SystemExit(
                "InfluxDB v2 source needs the 'influxdb-client' package: "
                "pip install influxdb-client"
            ) from exc

        self._org = org
        self._window = window
        self._client = InfluxDBClient(url=url, token=token, org=org, timeout=timeout_ms)
        self._query_api = self._client.query_api()

    def scopes(self) -> List[str]:
        buckets_api = self._client.buckets_api()
        buckets = buckets_api.find_buckets().buckets or []
        # Skip the internal monitoring buckets InfluxDB ships with.
        return [b.name for b in buckets if not b.name.startswith("_")]

    def measurements(self, scope: str) -> List[str]:
        flux = (
            f'import "influxdata/influxdb/schema"\n'
            f'schema.measurements(bucket: "{_esc(scope)}")'
        )
        return [r["_value"] for r in self._flat_records(flux)]

    def schema(self, scope: str, measurement: str) -> TableSchema:
        ts = TableSchema(table=measurement)
        tag_flux = (
            f'import "influxdata/influxdb/schema"\n'
            f'schema.measurementTagKeys(bucket: "{_esc(scope)}", '
            f'measurement: "{_esc(measurement)}")'
        )
        ts.tag_keys = [
            r["_value"]
            for r in self._flat_records(tag_flux)
            if not r["_value"].startswith("_")
        ]
        field_flux = (
            f'import "influxdata/influxdb/schema"\n'
            f'schema.measurementFieldKeys(bucket: "{_esc(scope)}", '
            f'measurement: "{_esc(measurement)}")'
        )
        for r in self._flat_records(field_flux):
            # Field types are inferred per row from the actual _value during the
            # data pass; default to FLOAT here and refine as values arrive.
            ts.field_types[r["_value"]] = FieldType.FLOAT
        return ts

    def rows(self, scope: str, measurement: str, schema: TableSchema) -> Iterator[Row]:
        oldest = self._oldest(scope, measurement)
        if oldest is None:
            return
        start = oldest
        now = datetime.now(timezone.utc)
        tag_set = set(schema.tag_keys)
        while start < now:
            stop = min(start + self._window, now)
            flux = (
                f'from(bucket: "{_esc(scope)}")\n'
                f"  |> range(start: {_rfc3339(start)}, stop: {_rfc3339(stop)})\n"
                f'  |> filter(fn: (r) => r._measurement == "{_esc(measurement)}")\n'
                f'  |> pivot(rowKey: ["_time"], columnKey: ["_field"], '
                f"valueColumn: \"_value\")"
            )
            for record in self._flat_records(flux):
                ts_ns = _ns(record.get("_time"))
                if ts_ns is None:
                    continue
                tags = {}
                fields = {}
                for key, value in record.items():
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
            start = stop

    def principals(self) -> List[Principal]:
        principals: List[Principal] = []
        try:
            auths = self._client.authorizations_api().find_authorizations()
        except Exception as exc:  # noqa: BLE001 - needs an operator/admin token
            log.warning("cannot read authorizations: %s; skipping ACL", exc)
            return principals
        bucket_id_to_name = self._bucket_id_index()
        for auth in auths or []:
            # Name the acl.conf user after the token's user/description.
            name = (
                getattr(auth, "user", None)
                or getattr(auth, "description", None)
                or getattr(auth, "id", "token")
            )
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

    def _oldest(self, scope: str, measurement: str) -> Optional[datetime]:
        # Find the earliest timestamp so windowing starts at real data, not an
        # arbitrary epoch that would scan years of empty ranges.
        flux = (
            f'from(bucket: "{_esc(scope)}")\n'
            f"  |> range(start: 0)\n"
            f'  |> filter(fn: (r) => r._measurement == "{_esc(measurement)}")\n'
            f'  |> keep(columns: ["_time"])\n'
            f"  |> first()"
        )
        records = self._flat_records(flux)
        if not records:
            return None
        return _to_dt(records[0].get("_time"))

    def _flat_records(self, flux: str):
        tables = self._query_api.query(flux, org=self._org)
        out = []
        for table in tables:
            for record in table.records:
                out.append(record.values)
        return out

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
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
