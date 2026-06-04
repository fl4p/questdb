"""Unit tests for the ha_van migration spot-checker's pure helpers.

The important one is ``diff_rows``: the verifier must catch BOTH a value that
disagrees AND a source row missing from QuestDB (silent data loss). The latter is
the reverse direction the original verifier lacked. Also covers the LP value
parser, the numeric/string equality, the timestamp formatting, and SQL escaping.
No QuestDB or InfluxDB is involved.

Run with the tool directory on the path:

    PYTHONPATH=. python3 tests/test_verify_ha_van.py
"""

import unittest

from verify_ha_van import (
    diff_rows,
    equalish,
    first_unescaped_space,
    influx_values,
    num,
    sql_str,
    us_to_iso,
)


class DiffRowsTests(unittest.TestCase):
    def test_all_match(self):
        qrows = [(1000, "5"), (2000, "6")]
        inf = {1000: "5", 2000: "6"}
        checked, mismatches, missing = diff_rows(qrows, inf, 1000, 3000)
        self.assertEqual(checked, 2)
        self.assertEqual(mismatches, [])
        self.assertEqual(missing, [])

    def test_value_mismatch_detected(self):
        _, mismatches, missing = diff_rows([(1000, "5")], {1000: "9"}, 1000, 3000)
        self.assertEqual(mismatches, [(1000, "5", "9")])
        self.assertEqual(missing, [])

    def test_row_missing_from_questdb_detected(self):
        # influx has 2000 but QuestDB does not -> the silent-data-loss signal.
        _, mismatches, missing = diff_rows(
            [(1000, "5")], {1000: "5", 2000: "6"}, 1000, 3000
        )
        self.assertEqual(mismatches, [])
        self.assertEqual(missing, [(2000, "6")])

    def test_inclusive_end_boundary_is_not_flagged_missing(self):
        # export-lp --end can be inclusive: a point exactly at `we` is outside the
        # half-open QuestDB window [ws, we) and must not count as missing.
        _, _, missing = diff_rows([(1000, "5")], {1000: "5", 3000: "x"}, 1000, 3000)
        self.assertEqual(missing, [])

    def test_point_before_window_start_is_ignored(self):
        _, _, missing = diff_rows([(1000, "5")], {500: "a", 1000: "5"}, 1000, 3000)
        self.assertEqual(missing, [])

    def test_questdb_row_absent_from_influx_is_a_mismatch(self):
        _, mismatches, _ = diff_rows([(1000, "5")], {}, 1000, 3000)
        self.assertEqual(mismatches, [(1000, "5", None)])


class InfluxValuesTests(unittest.TestCase):
    def test_parses_value_field_and_converts_ns_to_us(self):
        lines = ["pct,entity_id=sensor.a,domain=sensor value=87 1500000"]
        self.assertEqual(influx_values(lines, "sensor.a"), {1500: "87"})

    def test_non_value_fields_skipped(self):
        lines = ['pct,entity_id=sensor.a friendly_name="X" 1000000']
        self.assertEqual(influx_values(lines, "sensor.a"), {})

    def test_entity_substring_collision_excluded(self):
        # "sensor.a" must not match the row tagged "sensor.ab".
        lines = [
            "pct,entity_id=sensor.a value=1 1000000",
            "pct,entity_id=sensor.ab value=2 2000000",
        ]
        self.assertEqual(influx_values(lines, "sensor.a"), {1000: "1"})

    def test_malformed_or_missing_timestamp_skipped(self):
        self.assertEqual(influx_values(["nospace"], "x"), {})
        self.assertEqual(influx_values(["pct,entity_id=x value=1 notanumber"], "x"), {})


class NumTests(unittest.TestCase):
    def test_plain_and_int_suffix(self):
        self.assertEqual(num("87"), 87.0)
        self.assertEqual(num("87i"), 87.0)
        self.assertEqual(num("3.14"), 3.14)

    def test_non_numeric_returns_none(self):
        self.assertIsNone(num('"hello"'))
        self.assertIsNone(num(""))


class EqualishTests(unittest.TestCase):
    def test_numeric_within_tolerance(self):
        self.assertTrue(equalish("5", "5"))
        self.assertTrue(equalish("5.0000001", "5"))
        self.assertFalse(equalish("5", "6"))

    def test_string_compare_unquoted_qdb_vs_quoted_influx(self):
        self.assertTrue(equalish("on", '"on"'))
        self.assertFalse(equalish("on", '"off"'))

    def test_none_handling(self):
        self.assertTrue(equalish(None, None))
        self.assertFalse(equalish(None, "5"))
        self.assertFalse(equalish("5", None))


class MiscHelperTests(unittest.TestCase):
    def test_us_to_iso_floors_to_whole_second(self):
        self.assertEqual(us_to_iso(1_000_000), "1970-01-01T00:00:01Z")
        self.assertEqual(us_to_iso(1_999_999), "1970-01-01T00:00:01Z")

    def test_sql_str_escapes_single_quotes(self):
        self.assertEqual(sql_str("a'b"), "a''b")
        self.assertEqual(sql_str("plain"), "plain")

    def test_first_unescaped_space_skips_escaped(self):
        line = "%\\ available,entity_id=x value=5 9"
        self.assertEqual(line[: first_unescaped_space(line)], "%\\ available,entity_id=x")


if __name__ == "__main__":
    unittest.main()
