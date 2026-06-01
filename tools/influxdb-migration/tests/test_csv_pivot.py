"""Unit tests for the wide-CSV sink and its integration with the pivot.

These exercise the pure CSV shaping (head/tag parsing, line-protocol value
decoding, RFC-4180 quoting, nanosecond timestamp formatting) and the full
``merge_stream`` -> ``CsvSink`` path in both exact and downsample modes, writing
to a temp directory. No QuestDB is involved; schema resolution is a plain dict.

Run with the tool directory on the path:

    PYTHONPATH=. python3 tests/test_csv_pivot.py
"""

import os
import tempfile
import unittest

import csv_pivot
from csv_pivot import CsvSink, format_ts_iso_ns, parse_head
from pivot_lp import merge_stream
from qdb_admin import parse_schema_tables

_SQL = """
CREATE TABLE 'b_batmon' (
  timestamp TIMESTAMP_NS,
  did SYMBOL INDEX,
  voltage DOUBLE,
  cellcount LONG,
  charging BOOLEAN,
  note SYMBOL
) timestamp(timestamp) PARTITION BY DAY WAL;
"""


def _schemas():
    return {
        (m[2:] if m.startswith("b_") else m): s
        for m, s in parse_schema_tables(_SQL).items()
    }


class ParseHeadTests(unittest.TestCase):
    def test_measurement_only(self):
        self.assertEqual(parse_head("batmon"), ("batmon", []))

    def test_measurement_and_tags(self):
        meas, tags = parse_head("batmon,did=A,note=hello")
        self.assertEqual(meas, "batmon")
        self.assertEqual(tags, [("did", "A"), ("note", "hello")])

    def test_escaped_comma_in_tag_value(self):
        # In LP a comma inside a tag value is backslash-escaped; it must not split.
        meas, tags = parse_head("m,name=a\\,b,other=x")
        self.assertEqual(meas, "m")
        self.assertEqual(tags, [("name", "a,b"), ("other", "x")])

    def test_escaped_space_in_measurement(self):
        meas, tags = parse_head("we\\ ather,city=lon")
        self.assertEqual(meas, "we ather")
        self.assertEqual(tags, [("city", "lon")])


class ValueShapingTests(unittest.TestCase):
    def test_iso_ns_timestamp_keeps_nanoseconds(self):
        self.assertEqual(
            format_ts_iso_ns(1_700_000_000_123_456_789),
            "2023-11-14T22:13:20.123456789Z",
        )

    def test_iso_ns_zero_fraction(self):
        self.assertEqual(
            format_ts_iso_ns(1_700_000_000_000_000_000),
            "2023-11-14T22:13:20.000000000Z",
        )

    def test_unescape_lp_string(self):
        self.assertEqual(csv_pivot._unescape_lp_string('"hello"'), "hello")
        self.assertEqual(csv_pivot._unescape_lp_string('"a \\"q\\" b"'), 'a "q" b')
        self.assertEqual(csv_pivot._unescape_lp_string("3.5"), "3.5")

    def test_csv_quote(self):
        self.assertEqual(csv_pivot._csv_quote("plain", ","), "plain")
        self.assertEqual(csv_pivot._csv_quote("a,b", ","), '"a,b"')
        self.assertEqual(csv_pivot._csv_quote('a"b', ","), '"a""b"')
        self.assertEqual(csv_pivot._csv_quote("a\nb", ","), '"a\nb"')


class MergeToCsvTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _read(self, sink, measurement):
        with open(sink.paths[measurement], encoding="utf-8") as fh:
            return fh.read()

    def test_exact_merges_fields_of_one_point(self):
        lines = [
            "batmon,did=A voltage=3.27 100",
            "batmon,did=A cellcount=4i 100",
            "batmon,did=A charging=t 100",
            "batmon,did=A voltage=3.30 200",
        ]
        sink = CsvSink(self.dir, _schemas().get, prefix="b_", timestamp_mode="epoch-ns")
        pts, fl = merge_stream(iter(lines), "b_", None, 0, None, None, sink)
        self.assertEqual((pts, fl), (2, 4))
        self.assertEqual(
            self._read(sink, "batmon"),
            "timestamp,did,voltage,cellcount,charging,note\n"
            "100,A,3.27,4,true,\n"
            "200,A,3.30,,,\n",
        )

    def test_downsample_last_value_right_labeled(self):
        lines = [
            "batmon,did=A voltage=3.27 100",
            "batmon,did=A cellcount=4i 100",
            "batmon,did=A voltage=3.30 200",
        ]
        sink = CsvSink(self.dir, _schemas().get, prefix="b_", timestamp_mode="epoch-ns")
        pts, fl = merge_stream(iter(lines), "b_", None, 100, None, None, sink)
        self.assertEqual((pts, fl), (2, 3))
        # bucket [100,200) -> label 200 (voltage 3.27, cellcount 4); [200,300) -> 300.
        self.assertEqual(
            self._read(sink, "batmon"),
            "timestamp,did,voltage,cellcount,charging,note\n"
            "200,A,3.27,4,,\n"
            "300,A,3.30,,,\n",
        )

    def test_iso_ns_timestamp_mode(self):
        lines = ["batmon,did=A voltage=3.27 1700000000123456789"]
        sink = CsvSink(self.dir, _schemas().get, prefix="b_", timestamp_mode="iso-ns")
        merge_stream(iter(lines), "b_", None, 0, None, None, sink)
        body = self._read(sink, "batmon").splitlines()
        self.assertTrue(body[1].startswith("2023-11-14T22:13:20.123456789Z,A,3.27"))

    def test_string_field_space_not_quoted(self):
        # A SYMBOL/string field carrying a space survives the pivot (first/last
        # space split). RFC-4180 does NOT require quoting an internal space, so
        # the LP quotes are stripped and the bare value is written.
        lines = ['batmon,did=A note="hi there" 100']
        sink = CsvSink(self.dir, _schemas().get, prefix="b_", timestamp_mode="epoch-ns")
        merge_stream(iter(lines), "b_", None, 0, None, None, sink)
        self.assertIn("100,A,,,,hi there\n", self._read(sink, "batmon"))

    def test_unknown_measurement_skipped_no_file(self):
        lines = ["other,did=A v=1 100"]
        sink = CsvSink(self.dir, _schemas().get, prefix="b_", timestamp_mode="epoch-ns")
        pts, _ = merge_stream(iter(lines), "b_", None, 0, None, None, sink)
        self.assertEqual(pts, 1)  # pivot counted it, but the sink dropped it
        self.assertEqual(sink.paths, {})
        self.assertEqual(os.listdir(self.dir), [])

    def test_filename_is_prefixed_table(self):
        lines = ["batmon,did=A voltage=1 100"]
        sink = CsvSink(self.dir, _schemas().get, prefix="b_", timestamp_mode="epoch-ns")
        merge_stream(iter(lines), "b_", None, 0, None, None, sink)
        self.assertTrue(sink.paths["batmon"].endswith("b_batmon.csv"))

    def test_resolver_called_once_per_measurement(self):
        calls = []

        def resolve(m):
            calls.append(m)
            return _schemas().get(m)

        lines = [
            "batmon,did=A voltage=1 100",
            "batmon,did=A voltage=2 200",
            "batmon,did=B voltage=3 300",
        ]
        sink = CsvSink(self.dir, resolve, prefix="b_", timestamp_mode="epoch-ns")
        merge_stream(iter(lines), "b_", None, 0, None, None, sink)
        self.assertEqual(calls, ["batmon"])  # resolved once, then cached

    def test_bad_timestamp_mode_rejected(self):
        with self.assertRaises(ValueError):
            CsvSink(self.dir, _schemas().get, timestamp_mode="nope")


if __name__ == "__main__":
    unittest.main()
