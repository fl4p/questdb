"""Unit tests for the sorted-stream wide-row pivot and the WAL throttle.

These exercise the pure pivot/downsample logic with a counting feeder, and the
``_WalThrottle`` / ``_ThrottledFeeder`` pair with a fake ``urlopen`` so no real
QuestDB is involved. The throttle's ``time.sleep`` is monkeypatched to a recorder
so the pause loop runs instantly.

Run with the tool directory on the path:

    PYTHONPATH=. python3 tests/test_pivot_lp.py
"""

import json
import unittest

import pivot_lp
from pivot_lp import (
    SchemaCoercer,
    _ThrottledFeeder,
    _WalThrottle,
    merge_stream,
    parse_interval_ns,
)

# measurement -> {column: kind} used by the SchemaCoercer tests below.
_SCHEMA = {
    "batmon": {
        "voltage": "float",
        "num_samples": "float",
        "voltage_cell000": "int",
        "problem_code": "int",
        "switches_charge": "bool",
    }
}


class _CountingFeeder:
    """Collects the wide LP lines the pivot would POST, in order."""

    def __init__(self):
        self.lines = []

    def add(self, line):
        self.lines.append(line)

    def flush(self):
        pass


class _FakeResp:
    def __init__(self, payload=b""):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return self._payload


class ParseIntervalTests(unittest.TestCase):
    def test_off_values(self):
        for spec in ("", "0", "none", "OFF", None):
            self.assertEqual(parse_interval_ns(spec), 0)

    def test_suffixes(self):
        self.assertEqual(parse_interval_ns("10s"), 10_000_000_000)
        self.assertEqual(parse_interval_ns("1m"), 60_000_000_000)
        self.assertEqual(parse_interval_ns("1h"), 3_600_000_000_000)
        self.assertEqual(parse_interval_ns("1d"), 86_400_000_000_000)

    def test_bare_nanoseconds(self):
        self.assertEqual(parse_interval_ns("250"), 250)


class ExactPivotTests(unittest.TestCase):
    def test_merges_adjacent_same_key_fields_into_one_wide_line(self):
        lines = [
            "batmon,device=x current=-0.5 100",
            "batmon,device=x power=-13 100",
            "batmon,device=x voltage=26 100",
            "batmon,device=x current=0.1 200",
        ]
        feeder = _CountingFeeder()
        points, field_lines = merge_stream(lines, "p_", feeder, interval_ns=0)
        self.assertEqual((points, field_lines), (2, 4))
        self.assertEqual(
            feeder.lines,
            [
                "p_batmon,device=x current=-0.5,power=-13,voltage=26 100",
                "p_batmon,device=x current=0.1 200",
            ],
        )

    def test_different_heads_do_not_merge(self):
        lines = [
            "batmon,device=x current=1 100",
            "cells,device=x temp=2 100",
        ]
        feeder = _CountingFeeder()
        points, _ = merge_stream(lines, "", feeder, interval_ns=0)
        self.assertEqual(points, 2)


class DownsampleNoLookaheadTests(unittest.TestCase):
    def test_right_labels_bucket_end_and_keeps_last_value(self):
        # interval 100ns. Bucket [100,200) holds ts 120,150; [200,300) holds 250.
        lines = [
            "batmon,device=x current=-0.5 120",
            "batmon,device=x power=-13 150",
            "batmon,device=x current=-0.6 150",
            "batmon,device=x current=0.1 250",
        ]
        feeder = _CountingFeeder()
        points, field_lines = merge_stream(lines, "", feeder, interval_ns=100)
        self.assertEqual((points, field_lines), (2, 4))
        # First bucket stamped at END (200), last current in [100,200) is -0.6.
        self.assertEqual(
            feeder.lines[0], "batmon,device=x current=-0.6,power=-13 200"
        )
        # Second bucket [200,300) stamped at 300, holds only current=0.1.
        self.assertEqual(feeder.lines[1], "batmon,device=x current=0.1 300")

    def test_interleaved_series_in_one_bucket_flush_together(self):
        # Timestamp-sorted input interleaving two series 'a' and 'b' inside the
        # same bucket [0,100). The ts-ordered downsampler must capture BOTH and
        # flush them together at the bucket end when ts crosses into [100,200).
        # (A series-first accumulator would wrongly flush 'a' when 'b' appears.)
        lines = [
            "a,t=x v=1 10",
            "b,t=y v=2 12",
            "a,t=x v=3 15",
            "a,t=x v=9 110",
        ]
        feeder = _CountingFeeder()
        points, field_lines = merge_stream(lines, "", feeder, interval_ns=100)
        self.assertEqual((points, field_lines), (3, 4))
        # Bucket [0,100): a's last value is 3, b's is 2 -> both stamped at 100.
        self.assertEqual(
            sorted(feeder.lines[:2]),
            ["a,t=x v=3 100", "b,t=y v=2 100"],
        )
        # Bucket [100,200): a=9 stamped at 200.
        self.assertEqual(feeder.lines[2], "a,t=x v=9 200")

    def test_no_future_value_leaks_into_a_row(self):
        # A sample exactly at a bucket boundary (200) belongs to the NEXT bucket
        # [200,300) -> stamped 300, never to the row labeled 200.
        lines = [
            "m,t=a v=1 150",
            "m,t=a v=9 200",
        ]
        feeder = _CountingFeeder()
        merge_stream(lines, "", feeder, interval_ns=100)
        self.assertEqual(feeder.lines, ["m,t=a v=1 200", "m,t=a v=9 300"])


class WalThrottlePendingTests(unittest.TestCase):
    def _throttle_with(self, dataset, prefix="p_", batch_size=10_000):
        payload = json.dumps({"dataset": dataset}).encode("utf-8")

        def fake_urlopen(req, timeout=None):
            return _FakeResp(payload)

        self._orig = pivot_lp.urllib.request.urlopen
        pivot_lp.urllib.request.urlopen = fake_urlopen
        self.addCleanup(
            lambda: setattr(pivot_lp.urllib.request, "urlopen", self._orig)
        )
        return _WalThrottle(
            "http://qdb:9000",
            auth=None,
            prefix=prefix,
            batch_size=batch_size,
            high_rows=10_000_000,
            low_rows=5_000_000,
            poll_secs=1.0,
        )

    def test_pending_rows_sums_lag_for_prefixed_tables(self):
        # Columns: name, writerTxn, sequencerTxn.
        # p_a: lag 700 txns; p_b: lag 100 txns; other: ignored by prefix.
        ds = [
            ["p_a", 300, 1000],
            ["p_b", 0, 100],
            ["other", 0, 999_999],
        ]
        t = self._throttle_with(ds)
        # (700 + 100) txns * 10000 = 8,000,000 rows.
        self.assertEqual(t._pending_rows(), 8_000_000)

    def test_negative_lag_is_ignored(self):
        ds = [["p_a", 5, 3]]  # writer ahead (shouldn't happen) -> 0
        t = self._throttle_with(ds)
        self.assertEqual(t._pending_rows(), 0)

    def test_query_failure_returns_none(self):
        def boom(req, timeout=None):
            raise OSError("connection refused")

        self._orig = pivot_lp.urllib.request.urlopen
        pivot_lp.urllib.request.urlopen = boom
        self.addCleanup(
            lambda: setattr(pivot_lp.urllib.request, "urlopen", self._orig)
        )
        t = _WalThrottle(
            "http://qdb:9000", None, "p_", 10_000, 1, 1, 1.0
        )
        self.assertIsNone(t._pending_rows())


class WalThrottleWaitTests(unittest.TestCase):
    def setUp(self):
        self.slept = []
        self._orig_sleep = pivot_lp.time.sleep
        pivot_lp.time.sleep = lambda s: self.slept.append(s)
        self.addCleanup(lambda: setattr(pivot_lp.time, "sleep", self._orig_sleep))

    def _throttle(self, sequence, high=10_000_000, low=5_000_000):
        # sequence: successive return values of _pending_rows()
        it = iter(sequence)
        t = _WalThrottle("http://qdb:9000", None, "p_", 1, high, low, 1.0)
        t._pending_rows = lambda: next(it)
        return t

    def test_no_pause_when_below_high(self):
        t = self._throttle([4_000_000])
        t.wait_if_needed()
        self.assertEqual(self.slept, [])

    def test_pauses_until_below_low(self):
        # first read: over high -> pause. polls: 12M, 8M, then 4M (<= low) -> resume.
        t = self._throttle([12_000_000, 12_000_000, 8_000_000, 4_000_000])
        t.wait_if_needed()
        self.assertEqual(len(self.slept), 3)

    def test_lost_signal_resumes_instead_of_hanging(self):
        t = self._throttle([12_000_000, None])
        t.wait_if_needed()
        self.assertEqual(len(self.slept), 1)


class ThrottledFeederTests(unittest.TestCase):
    def setUp(self):
        self.posts = []

        def fake_urlopen(req, timeout=None):
            self.posts.append(req.data)
            return _FakeResp()

        self._orig = pivot_lp.urllib.request.urlopen
        pivot_lp.urllib.request.urlopen = fake_urlopen
        self.addCleanup(
            lambda: setattr(pivot_lp.urllib.request, "urlopen", self._orig)
        )

    def test_checks_backlog_every_n_batches(self):
        calls = {"n": 0}

        class _Stub:
            def wait_if_needed(self):
                calls["n"] += 1

        feeder = _ThrottledFeeder(
            "http://qdb:9000", batch_size=1, auth=None, throttle=_Stub(),
            check_every=3,
        )
        for i in range(6):
            feeder.add("m v=%d" % i)  # each add auto-flushes (batch_size=1)
        # 6 batches, checked every 3 -> 2 throttle consultations.
        self.assertEqual(calls["n"], 2)

    def test_empty_flush_does_not_consult_throttle(self):
        calls = {"n": 0}

        class _Stub:
            def wait_if_needed(self):
                calls["n"] += 1

        feeder = _ThrottledFeeder(
            "http://qdb:9000", batch_size=10, auth=None, throttle=_Stub(),
            check_every=1,
        )
        feeder.flush()  # nothing buffered
        self.assertEqual(calls["n"], 0)


class SchemaCoercerTests(unittest.TestCase):
    def setUp(self):
        self.co = SchemaCoercer({k: dict(v) for k, v in _SCHEMA.items()})

    def test_drops_unknown_column(self):
        self.assertIsNone(self.co.transform_fields("batmon", "temperatures_200=3.5"))

    def test_all_fields_dropped_returns_none(self):
        self.assertIsNone(
            self.co.transform_fields("batmon", "temperatures_1=1,temperatures_2=2")
        )

    def test_unknown_measurement_passthrough(self):
        self.assertEqual(self.co.transform_fields("other", "x=1,y=2"), "x=1,y=2")

    def test_bool_true_false_forms(self):
        self.assertEqual(self.co.transform_fields("batmon", "switches_charge=1"), "switches_charge=t")
        self.assertEqual(self.co.transform_fields("batmon", "switches_charge=0"), "switches_charge=f")
        self.assertEqual(self.co.transform_fields("batmon", "switches_charge=0.0"), "switches_charge=f")
        self.assertEqual(self.co.transform_fields("batmon", "switches_charge=t"), "switches_charge=t")
        self.assertEqual(self.co.transform_fields("batmon", "switches_charge=false"), "switches_charge=f")

    def test_int_from_integer_and_float_tokens(self):
        self.assertEqual(self.co.transform_fields("batmon", "voltage_cell000=3200i"), "voltage_cell000=3200i")
        self.assertEqual(self.co.transform_fields("batmon", "problem_code=5"), "problem_code=5i")

    def test_float_keeps_and_strips_trailing_i(self):
        self.assertEqual(self.co.transform_fields("batmon", "voltage=13.5"), "voltage=13.5")
        self.assertEqual(self.co.transform_fields("batmon", "num_samples=7i"), "num_samples=7")

    def test_mixed_line_drops_and_coerces(self):
        out = self.co.transform_fields(
            "batmon",
            "voltage=13.5,temperatures_9=1,switches_charge=1,voltage_cell000=3200i",
        )
        self.assertEqual(out, "voltage=13.5,switches_charge=t,voltage_cell000=3200i")

    def test_warns_once_per_column_on_bool_mismatch(self):
        with self.assertLogs(pivot_lp.log, level="WARNING") as cm:
            self.co.transform_fields("batmon", "switches_charge=2")  # nonzero -> t, warn
            self.co.transform_fields("batmon", "switches_charge=3")  # still coerced, NO 2nd warn
        self.assertEqual(len(cm.records), 1)
        self.assertEqual(self.co.transform_fields("batmon", "switches_charge=2"), "switches_charge=t")

    def test_warns_once_on_fractional_int(self):
        with self.assertLogs(pivot_lp.log, level="WARNING") as cm:
            self.assertEqual(self.co.transform_fields("batmon", "voltage_cell000=3.7"), "voltage_cell000=3i")
            self.co.transform_fields("batmon", "voltage_cell000=4.2")
        self.assertEqual(len(cm.records), 1)

    def test_non_numeric_into_typed_column_is_dropped(self):
        with self.assertLogs(pivot_lp.log, level="WARNING"):
            self.assertIsNone(self.co.transform_fields("batmon", "voltage_cell000=oops"))

    def test_dropped_cols_deduped(self):
        self.co.transform_fields("batmon", "temperatures_1=1")
        self.co.transform_fields("batmon", "temperatures_1=2")
        self.co.transform_fields("batmon", "temperatures_2=3")
        self.assertEqual(self.co.dropped_cols, {("batmon", "temperatures_1"), ("batmon", "temperatures_2")})


class MergeStreamSchemaTests(unittest.TestCase):
    def _coercer(self):
        return SchemaCoercer({k: dict(v) for k, v in _SCHEMA.items()})

    def test_exact_mode_drops_and_coerces(self):
        # long-format field-lines (one field each), adjacent for one point
        lines = [
            "batmon,did=A voltage=13.5 1000",
            "batmon,did=A temperatures_9=22.0 1000",
            "batmon,did=A switches_charge=1 1000",
            "batmon,did=A voltage_cell000=3200i 1000",
        ]
        feeder = _CountingFeeder()
        pts, fl = merge_stream(iter(lines), "x_", feeder, 0, self._coercer())
        self.assertEqual(pts, 1)
        self.assertEqual(fl, 4)
        self.assertEqual(len(feeder.lines), 1)
        body = feeder.lines[0]
        self.assertNotIn("temperatures_9", body)
        self.assertIn("switches_charge=t", body)
        self.assertIn("voltage_cell000=3200i", body)
        self.assertIn("voltage=13.5", body)

    def test_exact_mode_point_with_only_dropped_fields_is_skipped(self):
        lines = [
            "batmon,did=A temperatures_9=1 1000",
            "batmon,did=A temperatures_8=2 1000",
        ]
        feeder = _CountingFeeder()
        pts, _ = merge_stream(iter(lines), "x_", feeder, 0, self._coercer())
        self.assertEqual(pts, 0)
        self.assertEqual(feeder.lines, [])

    def test_downsample_mode_drops_and_coerces(self):
        # two samples in one 10s bucket; last value per field kept
        lines = [
            "batmon,did=A switches_charge=0 1000000000",
            "batmon,did=A temperatures_9=5 1000000000",
            "batmon,did=A switches_charge=1 2000000000",
            "batmon,did=A voltage_cell000=3201i 2000000000",
        ]
        feeder = _CountingFeeder()
        pts, _ = merge_stream(iter(lines), "x_", feeder, 10_000_000_000, self._coercer())
        self.assertEqual(pts, 1)
        body = feeder.lines[0]
        self.assertNotIn("temperatures_9", body)
        self.assertIn("switches_charge=t", body)  # last value wins
        self.assertIn("voltage_cell000=3201i", body)
        self.assertTrue(body.endswith(" 10000000000"))  # right-labeled bucket end


if __name__ == "__main__":
    unittest.main()
