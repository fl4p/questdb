"""Keyset-pagination correctness tests for the v1 reader.

These use a fake InfluxDB client (no server, no influxdb package) that mimics
InfluxQL ``ORDER BY time ASC LIMIT`` and ``WHERE time = <t>`` semantics, so they
run in the dependency-free unit suite. They target the boundary hole where many
series share one timestamp at a page edge -- in particular where a single
timestamp has MORE rows than the page size, which a naive "step past max_t"
pager would silently drop.
"""

import re
import unittest

from model import FieldType, TableSchema
from readers.v1 import V1Reader


class _FakePoints:
    def __init__(self, pts):
        self._pts = pts

    def get_points(self):
        return iter(self._pts)


class _FakeV1Client:
    """Answers exactly the two query shapes V1Reader.rows() issues."""

    def __init__(self, points):
        # InfluxDB returns rows ordered by time; keep a stable order within a ts.
        self.points = sorted(points, key=lambda p: p["time"])
        self.queries = []

    def switch_database(self, _db):
        pass

    def query(self, q, epoch=None):
        self.queries.append(q)
        exact = re.search(r"WHERE time = (\d+)", q)
        if exact and "LIMIT" not in q:
            t = int(exact.group(1))
            return _FakePoints([dict(p) for p in self.points if p["time"] == t])
        cursor = re.search(r"WHERE time >= (\d+)", q)
        limit = re.search(r"LIMIT (\d+)", q)
        lo = int(cursor.group(1)) if cursor else None
        page = int(limit.group(1))
        selected = [p for p in self.points if lo is None or p["time"] >= lo]
        return _FakePoints([dict(p) for p in selected[:page]])


def _read(points, page):
    reader = V1Reader.__new__(V1Reader)  # bypass __init__ (no real client needed)
    reader._client = _FakeV1Client(points)
    reader._page_size = page
    schema = TableSchema(
        table="m", tag_keys=["host"], field_types={"v": FieldType.FLOAT}
    )
    rows = list(reader.rows("db", "m", schema))
    return rows, reader._client


def _dataset():
    # 7 rows across 3 timestamps; t=100 holds 4 rows (4 series).
    pts = []
    for h in range(4):
        pts.append({"time": 100, "host": f"h{h}", "v": float(h)})
    pts.append({"time": 200, "host": "h0", "v": 10.0})
    pts.append({"time": 200, "host": "h1", "v": 11.0})
    pts.append({"time": 300, "host": "h0", "v": 20.0})
    return pts


class V1PaginationTests(unittest.TestCase):
    def _keys(self, rows):
        return sorted((r.ts_ns, r.tags["host"]) for r in rows)

    def test_single_timestamp_denser_than_page_loses_nothing(self):
        # page=2 < 4 rows at t=100: the exact-timestamp refetch must kick in.
        # A naive pager would drop h2@100 and h3@100.
        rows, _ = _read(_dataset(), page=2)
        self.assertEqual(len(rows), 7)
        self.assertEqual(self._keys(rows), self._keys_from_points(_dataset()))

    def test_no_loss_or_dupes_at_every_page_size(self):
        expected = self._keys_from_points(_dataset())
        for page in (1, 2, 3, 4, 5, 6, 7, 100):
            rows, _ = _read(_dataset(), page=page)
            keys = self._keys(rows)
            self.assertEqual(len(keys), 7, f"row count wrong at page={page}")
            self.assertEqual(len(set(keys)), 7, f"duplicate row at page={page}")
            self.assertEqual(keys, expected, f"content mismatch at page={page}")

    def test_terminates_and_advances(self):
        # Guards against an infinite loop on the dense-timestamp branch.
        _, client = _read(_dataset(), page=1)
        self.assertLess(len(client.queries), 50)

    def _keys_from_points(self, pts):
        return sorted((p["time"], p["host"]) for p in pts)


if __name__ == "__main__":
    unittest.main(verbosity=2)
