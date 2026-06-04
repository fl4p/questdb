"""Unit tests for the line-protocol measurement-name sanitizer.

Covers the pure helpers (name mapping, the generic fallback, escaped-space-aware
splitting, head rebuilding) and the two end-to-end stdin->stdout modes (plain
measurement rewrite, and --value-only which keeps only the `value` field plus the
kept tags). No QuestDB or InfluxDB is involved.

Run with the tool directory on the path:

    PYTHONPATH=. python3 tests/test_sanitize_lp.py
"""

import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

import sanitize_lp
from sanitize_lp import (
    _first_unescaped_space,
    generic_sanitize,
    rebuild_head,
    sanitized,
    split_line,
)


def run_main(stdin_text, argv):
    """Drive sanitize_lp.main() with a fake stdin; return (stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.object(sanitize_lp.sys, "stdin", io.StringIO(stdin_text)), \
            redirect_stdout(out), redirect_stderr(err):
        rc = sanitize_lp.main(argv)
    assert rc == 0
    return out.getvalue(), err.getvalue()


class GenericSanitizeTests(unittest.TestCase):
    def test_unit_symbols_become_words(self):
        self.assertEqual(generic_sanitize("%/d"), "pct_per_d")
        self.assertEqual(generic_sanitize("kWh/h"), "kWh_per_h")
        self.assertEqual(generic_sanitize("°C"), "degC")
        self.assertEqual(generic_sanitize("º"), "deg")

    def test_illegal_chars_collapse_to_single_underscore(self):
        self.assertEqual(generic_sanitize("pending update(s)"), "pending_update_s")
        self.assertEqual(generic_sanitize("a   b"), "a_b")

    def test_leading_digit_is_prefixed(self):
        self.assertEqual(generic_sanitize("3phase"), "m_3phase")

    def test_empty_after_stripping_is_unnamed(self):
        self.assertEqual(generic_sanitize("()!"), "unnamed")


class SanitizedTests(unittest.TestCase):
    def setUp(self):
        sanitize_lp._warned.clear()

    def test_mapped_names(self):
        self.assertEqual(sanitized("%"), "pct")
        self.assertEqual(sanitized("V"), "V")  # identity, pinned
        self.assertEqual(sanitized("pending update(s)"), "pending_updates")

    def test_skip_list_returns_none(self):
        self.assertIsNone(sanitized("ºC"))  # U+00BA orphan

    def test_unknown_falls_back_to_generic_and_warns_once(self):
        out = io.StringIO()
        with redirect_stderr(out):
            self.assertEqual(sanitized("mV/x"), "mV_per_x")
            self.assertEqual(sanitized("mV/x"), "mV_per_x")
        # warned exactly once for the same unknown name
        self.assertEqual(out.getvalue().count("unknown measurement"), 1)


class FirstUnescapedSpaceTests(unittest.TestCase):
    def test_plain_space(self):
        self.assertEqual(_first_unescaped_space("a,b=c d=1 9"), 5)

    def test_escaped_space_is_skipped(self):
        # "%\ available,entity_id=x value=5 9": the escaped space inside the
        # measurement must NOT be taken as the head/fields boundary.
        line = "%\\ available,entity_id=x value=5 9"
        idx = _first_unescaped_space(line)
        self.assertEqual(line[:idx], "%\\ available,entity_id=x")

    def test_none(self):
        self.assertEqual(_first_unescaped_space("nospace"), -1)


class SplitLineTests(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(split_line("m,t=v f=1 100"), ("m,t=v", "f=1", "100"))

    def test_quoted_string_value_with_internal_space(self):
        head, field, ts = split_line('m,t=v value="hello world" 100')
        self.assertEqual(head, "m,t=v")
        self.assertEqual(field, 'value="hello world"')
        self.assertEqual(ts, "100")

    def test_trailing_space_does_not_produce_empty_timestamp(self):
        # Regression: rstrip() (not rstrip('\n')) so a stray trailing space does
        # not push rfind(' ') past the real timestamp and emit an empty ts.
        self.assertEqual(split_line("m,t=v f=1 100 \n"), ("m,t=v", "f=1", "100"))

    def test_line_without_timestamp_is_rejected(self):
        self.assertIsNone(split_line("m,t=v f=1\n"))

    def test_line_without_space_is_rejected(self):
        self.assertIsNone(split_line("measurementonly"))


class RebuildHeadTests(unittest.TestCase):
    def test_drops_unkept_tags_and_replaces_measurement(self):
        head = "degC,entity_id=sensor.t,domain=sensor,friendly_name=Foo"
        self.assertEqual(
            rebuild_head(head, "degC", {"entity_id", "domain"}),
            "degC,entity_id=sensor.t,domain=sensor",
        )

    def test_no_kept_tags_yields_bare_measurement(self):
        self.assertEqual(rebuild_head("m,foo=bar", "m", {"entity_id"}), "m")

    def test_measurement_with_no_tags(self):
        self.assertEqual(rebuild_head("m", "m", {"entity_id"}), "m")


class MainPlainModeTests(unittest.TestCase):
    def setUp(self):
        sanitize_lp._warned.clear()

    def test_rewrites_measurement_and_drops_skip_list(self):
        stdin = (
            "V,entity_id=a value=1 100\n"
            "ºC,foo=bar value=9 100\n"            # skip-listed -> dropped
            "%\\ available,entity_id=b value=2 200\n"  # escaped-space unit name
        )
        out, err = run_main(stdin, [])
        self.assertEqual(
            out,
            "V,entity_id=a value=1 100\n"
            "pct_available,entity_id=b value=2 200\n",
        )
        self.assertIn("kept 2 lines, dropped 1", err)

    def test_comment_lines_are_skipped(self):
        out, _ = run_main("#group\nV,entity_id=a value=1 100\n", [])
        self.assertEqual(out, "V,entity_id=a value=1 100\n")


class MainValueOnlyModeTests(unittest.TestCase):
    def setUp(self):
        sanitize_lp._warned.clear()

    def test_keeps_only_value_field_and_kept_tags(self):
        stdin = (
            "%,entity_id=sensor.batt,domain=sensor value=87 1000\n"
            '%,entity_id=sensor.batt,domain=sensor friendly_name="Battery" 1000\n'
            "V,entity_id=sensor.v value=12.5 2000\n"
        )
        out, err = run_main(stdin, ["--value-only"])
        self.assertEqual(
            out,
            "pct,entity_id=sensor.batt,domain=sensor value=87 1000\n"
            "V,entity_id=sensor.v value=12.5 2000\n",
        )
        self.assertIn("kept 2 lines, dropped 1", err)

    def test_value_only_strips_dirty_attribute_tags(self):
        # an HA attribute tag with an illegal column name must be dropped even on
        # a kept `value` line.
        stdin = "%,entity_id=x,domain=sensor,unit=pct value=5 300\n"
        out, _ = run_main(stdin, ["--value-only", "--keep-tags", "entity_id,domain"])
        self.assertEqual(out, "pct,entity_id=x,domain=sensor value=5 300\n")

    def test_value_only_trailing_space_emits_clean_timestamp(self):
        # the parse fix must not leak a trailing space / empty ts downstream.
        out, _ = run_main("V,entity_id=x value=5 300 \n", ["--value-only"])
        self.assertEqual(out, "V,entity_id=x value=5 300\n")


if __name__ == "__main__":
    unittest.main()
