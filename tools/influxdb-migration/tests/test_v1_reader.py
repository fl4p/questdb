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

_WINDOW_NS = 10  # tiny window so the small fake datasets span several windows


class _FakePoints:
    def __init__(self, pts):
        self._pts = pts

    def get_points(self):
        return iter(self._pts)


class _FakeV1Client:
    """Answers the query shapes V1Reader issues: the windowed/cursor SELECT, the
    exact-timestamp refetch, and the ASC/DESC LIMIT 1 oldest/newest probes."""

    def __init__(self, points):
        # InfluxDB returns rows ordered by time; keep a stable order within a ts.
        self.points = sorted(points, key=lambda p: p["time"])
        self.queries = []
        self.databases = []

    def switch_database(self, _db):
        pass

    def query(self, q, epoch=None, database=None):
        # The data path must pass database=scope (never switch_database) so it is
        # safe under the worker thread pool; record it so a test can assert it.
        self.queries.append(q)
        self.databases.append(database)
        exact = re.search(r"WHERE time = (\d+)", q)
        if exact and "LIMIT" not in q:
            t = int(exact.group(1))
            return _FakePoints([dict(p) for p in self.points if p["time"] == t])
        lo_m = re.search(r"time >= (\d+)", q)
        hi_m = re.search(r"time < (\d+)", q)
        limit = re.search(r"LIMIT (\d+)", q)
        lo = int(lo_m.group(1)) if lo_m else None
        hi = int(hi_m.group(1)) if hi_m else None
        page = int(limit.group(1))
        selected = [
            p
            for p in self.points
            if (lo is None or p["time"] >= lo) and (hi is None or p["time"] < hi)
        ]
        if "ORDER BY time DESC" in q:
            selected = list(reversed(selected))
        return _FakePoints([dict(p) for p in selected[:page]])


def _make_reader(points, page, window_ns=_WINDOW_NS):
    reader = V1Reader.__new__(V1Reader)  # bypass __init__ (no real client needed)
    reader._client = _FakeV1Client(points)
    reader._page_size = page
    reader._window_ns = window_ns
    return reader


def _schema():
    return TableSchema(
        table="m", tag_keys=["host"], field_types={"v": FieldType.FLOAT}
    )


def _read(points, page, window_ns=_WINDOW_NS):
    reader = _make_reader(points, page, window_ns)
    rows = list(reader.rows("db", "m", _schema()))
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


class V1PlanWindowsTests(unittest.TestCase):
    def test_empty_measurement_returns_empty_list(self):
        reader = _make_reader([], page=10)
        self.assertEqual(reader.plan_windows("db", "m"), [])

    def test_windows_cover_span_with_shared_half_open_bounds(self):
        # Data at t in {100, 200, 300}; window=10ns -> oldest=100, newest=300,
        # end=301. Windows tile [100, 301) in 10ns half-open steps with shared
        # edges; the final window is clamped to end (301) so newest is included.
        reader = _make_reader(_dataset(), page=10, window_ns=10)
        windows = reader.plan_windows("db", "m")
        self.assertEqual(windows[0], (100, 110))
        self.assertEqual(windows[-1][1], 301)  # exclusive stop includes newest
        # contiguous: every window's stop is the next window's start
        for (s0, e0), (s1, e1) in zip(windows, windows[1:]):
            self.assertEqual(e0, s1)
        # full coverage from oldest to end
        self.assertEqual(windows[0][0], 100)
        self.assertEqual((301 - 100 + 9) // 10, len(windows))  # ceil(span/window)

    def test_single_point_yields_one_window_including_it(self):
        reader = _make_reader([{"time": 500, "host": "h0", "v": 1.0}], page=10)
        windows = reader.plan_windows("db", "m")
        self.assertEqual(len(windows), 1)
        s, e = windows[0]
        self.assertTrue(s <= 500 < e)

    def test_probe_queries_pass_database_not_switch(self):
        reader = _make_reader(_dataset(), page=10)
        reader.plan_windows("db", "m")
        # Every query the plan probes issued carried database="db".
        self.assertTrue(all(d == "db" for d in reader._client.databases))


class V1RowsWindowTests(unittest.TestCase):
    def _keys(self, rows):
        return sorted((r.ts_ns, r.tags["host"]) for r in rows)

    def test_window_reads_only_its_own_points(self):
        # [100, 250) must capture t=100 (4 rows) and t=200 (2 rows), exclude 300.
        reader = _make_reader(_dataset(), page=10)
        rows = list(reader.rows_window("db", "m", _schema(), (100, 250)))
        self.assertEqual(
            self._keys(rows),
            sorted((p["time"], p["host"]) for p in _dataset() if 100 <= p["time"] < 250),
        )

    def test_boundary_point_lands_in_exactly_one_window(self):
        # t=200 sits on the shared edge of [100, 200) and [200, 300): the
        # half-open bound puts it only in the upper window, never both.
        reader = _make_reader(_dataset(), page=10)
        lower = list(reader.rows_window("db", "m", _schema(), (100, 200)))
        upper = list(reader.rows_window("db", "m", _schema(), (200, 300)))
        self.assertTrue(all(r.ts_ns < 200 for r in lower))
        self.assertIn(200, {r.ts_ns for r in upper})
        # no overlap
        self.assertEqual(
            set(self._keys(lower)) & set(self._keys(upper)), set()
        )

    def test_holdback_works_inside_a_window(self):
        # page=2 < 4 rows at t=100 inside window [100, 250): the hold-back /
        # exact-refetch path must still recover every row.
        reader = _make_reader(_dataset(), page=2)
        rows = list(reader.rows_window("db", "m", _schema(), (100, 250)))
        self.assertEqual(len(rows), 6)  # 4 at t=100 + 2 at t=200
        self.assertEqual(len(set(self._keys(rows))), 6)

    def test_data_path_uses_database_kwarg_only(self):
        reader = _make_reader(_dataset(), page=2)
        list(reader.rows_window("db", "m", _schema(), (100, 250)))
        # switch_database is never the mechanism; database=scope rides every call.
        self.assertTrue(reader._client.databases)
        self.assertTrue(all(d == "db" for d in reader._client.databases))


class V1WindowedEqualsSinglePassTests(unittest.TestCase):
    """The windowed rows() must equal the old single-pass output: no loss, no
    duplication, regardless of how many windows the span is sliced into."""

    def _keys(self, rows):
        return sorted((r.ts_ns, r.tags["host"]) for r in rows)

    def _single_pass(self, points):
        # A reference reader with one giant window == the pre-windowing behavior.
        reader = _make_reader(points, page=10, window_ns=10**9)
        windows = reader.plan_windows("db", "m")
        self.assertEqual(len(windows), 1)  # everything in one unit
        return list(reader.rows("db", "m", _schema()))

    def test_all_windows_union_equals_single_pass(self):
        points = _dataset()
        expected = self._keys(self._single_pass(points))
        for page in (1, 2, 3, 7, 100):
            for window_ns in (1, 5, 10, 50, 1000):
                reader = _make_reader(points, page=page, window_ns=window_ns)
                # Drive each window independently, as the orchestrator does.
                rows = []
                for w in reader.plan_windows("db", "m"):
                    rows.extend(reader.rows_window("db", "m", _schema(), w))
                keys = self._keys(rows)
                msg = f"page={page} window_ns={window_ns}"
                self.assertEqual(len(keys), 7, "row count wrong: " + msg)
                self.assertEqual(len(set(keys)), 7, "duplicate row: " + msg)
                self.assertEqual(keys, expected, "content mismatch: " + msg)


if __name__ == "__main__":
    unittest.main(verbosity=2)
