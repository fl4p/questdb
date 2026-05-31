"""v2 reader tests using fake clients (no server, no influxdb-client package).

Each test locks in a behavior whose absence was a real bug found only in live
runs, so a regression now fails fast in the unit suite:

- schema() must pass ``start: 0`` (else the Flux schema helpers default to the
  last 30 days and silently miss backfilled data -> tags land as fields).
- rows() must read in bounded time WINDOWS, never a single unbounded
  ``range(start: 0)`` data query (which makes InfluxDB compute the whole pivot
  before streaming and blows the client read timeout at scale).
- rows() must lose/duplicate nothing across window boundaries, including a
  point exactly on a boundary.
- principals() must name users after the token DESCRIPTION, not ``auth.user``
  (which is shared across a user's tokens and collides).
"""

import re
import unittest
from datetime import datetime, timedelta, timezone

from model import FieldType, TableSchema
from readers.v2 import V2Reader

UTC = timezone.utc


class _Rec:
    def __init__(self, values):
        self.values = values


class _Tbl:
    def __init__(self, records):
        self.records = records


def _parse(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)


class _FakeQueryApi:
    """Mimics the InfluxDB v2 query API over an in-memory pivoted dataset."""

    def __init__(self, dataset, measurements, tag_keys, field_keys):
        self.dataset = dataset  # list of {"_time": datetime, **cols}
        self.measurements = measurements
        self.tag_keys = tag_keys
        self.field_keys = field_keys
        self.fluxes = []

    def query(self, flux, org=None):
        # The reader reads everything through query() (materialized per window).
        self.fluxes.append(flux)
        if "schema.measurements" in flux:
            return [_Tbl([_Rec({"_value": m}) for m in self.measurements])]
        if "measurementTagKeys" in flux:
            return [_Tbl([_Rec({"_value": k}) for k in self.tag_keys])]
        if "measurementFieldKeys" in flux:
            return [_Tbl([_Rec({"_value": k}) for k in self.field_keys])]
        if "first()" in flux:
            if not self.dataset:
                return [_Tbl([])]
            return [_Tbl([_Rec({"_time": min(r["_time"] for r in self.dataset)})])]
        if "last()" in flux:
            if not self.dataset:
                return [_Tbl([])]
            return [_Tbl([_Rec({"_time": max(r["_time"] for r in self.dataset)})])]
        m = re.search(r"range\(start: (\S+), stop: (\S+)\)", flux)
        if not m:
            # An unbounded data query (range(start: 0) + pivot) is the very
            # regression these tests guard against -- it times out at scale.
            raise AssertionError(f"unbounded v2 data query would time out:\n{flux}")
        start, stop = _parse(m.group(1)), _parse(m.group(2))
        return [_Tbl([_Rec(dict(r)) for r in self.dataset if start <= r["_time"] < stop])]


def _reader(dataset, tag_keys, field_keys, window_min=10):
    reader = V2Reader.__new__(V2Reader)  # bypass __init__ (no real client)
    reader._org = "org"
    reader._window = timedelta(minutes=window_min)
    api = _FakeQueryApi(dataset, ["big"], tag_keys, field_keys)
    reader._query_api = api
    return reader, api


class V2SchemaTests(unittest.TestCase):
    def test_schema_queries_pass_start_zero(self):
        reader, api = _reader([], ["host"], ["val"])
        reader.schema("b", "big")
        tagq = next(f for f in api.fluxes if "measurementTagKeys" in f)
        fieldq = next(f for f in api.fluxes if "measurementFieldKeys" in f)
        self.assertIn("start: 0", tagq)
        self.assertIn("start: 0", fieldq)


class V2RowsTests(unittest.TestCase):
    def _dataset(self):
        t0 = datetime(2023, 11, 14, 0, 0, 0, tzinfo=UTC)
        return [
            {"_time": t0, "_measurement": "big", "host": "h0", "val": 1.0},
            {"_time": t0 + timedelta(minutes=5), "_measurement": "big", "host": "h0", "val": 2.0},
            {"_time": t0 + timedelta(minutes=10), "_measurement": "big", "host": "h1", "val": 3.0},  # boundary
            {"_time": t0 + timedelta(minutes=12), "_measurement": "big", "host": "h1", "val": 4.0},
            {"_time": t0 + timedelta(minutes=25), "_measurement": "big", "host": "h0", "val": 5.0},
        ]

    def _run(self):
        reader, api = _reader(self._dataset(), ["host"], ["val"], window_min=10)
        schema = TableSchema(table="big", tag_keys=["host"], field_types={"val": FieldType.FLOAT})
        return list(reader.rows("b", "big", schema)), api

    def test_windowed_reads_no_loss_no_dup(self):
        rows, _ = self._run()
        keys = sorted((r.ts_ns, r.tags["host"], r.fields["val"]) for r in rows)
        self.assertEqual(len(rows), 5)
        self.assertEqual(len(set(r.ts_ns for r in rows)), 5)  # boundary point not duped
        # tag vs field classification
        self.assertTrue(all("host" in r.tags and "val" in r.fields for r in rows))

    def test_issues_multiple_bounded_windows_never_unbounded(self):
        # If rows() ever reverts to a single range(start: 0) data query, the
        # fake raises AssertionError; reaching here means it stayed windowed.
        _, api = self._run()
        windows = [f for f in api.fluxes if re.search(r"range\(start: \S+, stop: \S+\)", f)]
        self.assertGreaterEqual(len(windows), 3)  # ~25 min of data, 10 min window

    def test_ns_timestamp_conversion(self):
        rows, _ = self._run()
        t0 = datetime(2023, 11, 14, 0, 0, 0, tzinfo=UTC)
        self.assertIn(int(t0.timestamp() * 1_000_000_000), {r.ts_ns for r in rows})


class _Resource:
    def __init__(self, type_, name=None, id=None, org_id=None):
        self.type = type_
        self.name = name
        self.id = id
        self.org_id = org_id


class _Perm:
    def __init__(self, action, resource):
        self.action = action
        self.resource = resource


class _Auth:
    def __init__(self, description, user, org_id, permissions):
        self.description = description
        self.user = user
        self.org_id = org_id
        self.permissions = permissions


class _Bucket:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class _FakeClient:
    def __init__(self, auths, buckets):
        self._auths = auths
        self._buckets = buckets

    def authorizations_api(self):
        outer = self

        class _A:
            def find_authorizations(self):
                return outer._auths

        return _A()

    def buckets_api(self):
        outer = self

        class _B:
            def find_buckets(self):
                class _L:
                    buckets = outer._buckets

                return _L()

        return _B()


class V2PrincipalsTests(unittest.TestCase):
    def _reader(self):
        buckets = [_Bucket("BID", "mybucket")]
        admin = _Auth(
            "admin's Token", "admin", "ORG",
            [_Perm("read", _Resource("buckets")), _Perm("write", _Resource("buckets"))],
        )
        reader_tok = _Auth(
            "reader-token", "admin", "ORG",
            [_Perm("read", _Resource("buckets", id="BID", org_id="ORG"))],
        )
        r = V2Reader.__new__(V2Reader)
        r._org = "ORG"
        r._client = _FakeClient([admin, reader_tok], buckets)
        return r

    def test_named_by_description_not_user(self):
        principals = self._reader().principals()
        names = {p.name for p in principals}
        self.assertEqual(names, {"admin's Token", "reader-token"})
        self.assertNotIn("admin", names)  # auth.user would have collided

    def test_all_access_is_admin_scoped_is_grant(self):
        by_name = {p.name: p for p in self._reader().principals()}
        self.assertTrue(by_name["admin's Token"].is_admin)
        rt = by_name["reader-token"]
        self.assertFalse(rt.is_admin)
        self.assertEqual([(g.scope, g.access.value) for g in rt.grants], [("mybucket", "ro")])


if __name__ == "__main__":
    unittest.main(verbosity=2)
