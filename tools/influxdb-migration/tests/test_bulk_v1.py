"""Unit tests for the v1 direct-TSM bulk import path (``bulk_v1.py``).

These exercise the pure helpers with no real ``influx_inspect`` and no real
QuestDB: the export stream is injected as a list of lines via the ``runner``
seam, and the ILP-over-HTTP feeder's ``urlopen`` is monkeypatched to a recorder.
The focus is the measurement-prefix rewrite against tricky line-protocol inputs
(escaped commas/spaces in the measurement, tags with commas, fields with spaces
and quoted strings, multiple lines).

Run with the tool directory on the path:

    PYTHONPATH=. python3 tests/test_bulk_v1.py
"""

import unittest

import bulk_v1
from bulk_v1 import (
    build_export_cmd,
    measurement_of,
    parse_basic_or_token_auth,
    rewrite_line,
    rewrite_measurement,
    run_bulk_import,
)
from model import InvalidScopeName


class MeasurementParseTests(unittest.TestCase):
    def test_first_unescaped_comma_starts_tags(self):
        self.assertEqual(measurement_of("cpu,host=a v=1 5"), "cpu")

    def test_first_unescaped_space_starts_fields_when_no_tags(self):
        self.assertEqual(measurement_of("cpu v=1 5"), "cpu")

    def test_escaped_comma_stays_in_measurement(self):
        # "weather\\,sensor" is ONE measurement named "weather,sensor".
        self.assertEqual(
            measurement_of("weather\\,sensor,host=a v=1 5"), "weather,sensor"
        )

    def test_escaped_space_stays_in_measurement(self):
        self.assertEqual(
            measurement_of("disk\\ usage,host=a v=1 5"), "disk usage"
        )

    def test_escaped_comma_and_space_together(self):
        self.assertEqual(
            measurement_of("a\\,b\\ c,host=h v=1 5"), "a,b c"
        )

    def test_no_tags_no_timestamp(self):
        self.assertEqual(measurement_of("cpu v=1"), "cpu")

    def test_escaped_backslash_then_real_delimiter(self):
        # "m\\\\" is measurement "m\\" (escaped backslash), then the space is a
        # real delimiter starting the fields. A naive "preceded by backslash"
        # check would wrongly swallow the space.
        self.assertEqual(measurement_of("m\\\\ v=1 5"), "m\\")


class RewriteMeasurementTests(unittest.TestCase):
    def test_plain_measurement(self):
        self.assertEqual(rewrite_measurement("cpu", "mydb_"), "mydb_cpu")

    def test_preserves_escaped_comma(self):
        # literal name "weather,sensor" -> "mydb_weather,sensor", re-escaped.
        self.assertEqual(
            rewrite_measurement("weather\\,sensor", "mydb_"),
            "mydb_weather\\,sensor",
        )

    def test_preserves_escaped_space(self):
        self.assertEqual(
            rewrite_measurement("disk\\ usage", "mydb_"), "mydb_disk\\ usage"
        )

    def test_empty_prefix_is_identity_for_clean_name(self):
        self.assertEqual(rewrite_measurement("cpu", ""), "cpu")


class RewriteLineTests(unittest.TestCase):
    def test_only_measurement_token_changes(self):
        line = "cpu,host=a,region=us v=1,b=2i 1500000000000000000"
        self.assertEqual(
            rewrite_line(line, "mydb_"),
            "mydb_cpu,host=a,region=us v=1,b=2i 1500000000000000000",
        )

    def test_no_tags(self):
        self.assertEqual(
            rewrite_line("cpu v=1 5", "mydb_"), "mydb_cpu v=1 5"
        )

    def test_tag_values_with_commas_are_untouched(self):
        # Commas inside the tag set must not be re-parsed: only the measurement
        # (up to the FIRST unescaped comma) is rewritten; everything after the
        # cut is copied verbatim.
        line = "cpu,host=a,path=/etc,role=db v=1 5"
        self.assertEqual(
            rewrite_line(line, "p_"), "p_cpu,host=a,path=/etc,role=db v=1 5"
        )

    def test_field_string_with_spaces_is_untouched(self):
        # A quoted string field can contain spaces; the rewrite must not touch
        # anything past the measurement.
        line = 'log,host=a msg="hello world, friend" 5'
        self.assertEqual(
            rewrite_line(line, "p_"), 'p_log,host=a msg="hello world, friend" 5'
        )

    def test_escaped_measurement_with_tags_and_fields(self):
        line = "a\\,b\\ c,host=h\\ x v=1,s=\"a b\" 5"
        self.assertEqual(
            rewrite_line(line, "db_"), "db_a\\,b\\ c,host=h\\ x v=1,s=\"a b\" 5"
        )

    def test_comment_and_blank_lines_pass_through(self):
        self.assertEqual(rewrite_line("# a comment", "p_"), "# a comment")
        self.assertEqual(rewrite_line("", "p_"), "")


class BuildExportCmdTests(unittest.TestCase):
    def test_minimal_lponly_to_stdout(self):
        cmd = build_export_cmd("mydb", "/data", "/wal")
        self.assertEqual(
            cmd,
            [
                "influx_inspect", "export", "-lponly",
                "-database", "mydb",
                "-datadir", "/data",
                "-waldir", "/wal",
                "-out", "-",
            ],
        )

    def test_optional_flags(self):
        cmd = build_export_cmd(
            "mydb", "/data", "/wal",
            retention="autogen", start="S", end="E", compress=True,
            binary="/opt/influx_inspect",
        )
        self.assertEqual(cmd[0], "/opt/influx_inspect")
        self.assertIn("-retention", cmd)
        self.assertEqual(cmd[cmd.index("-retention") + 1], "autogen")
        self.assertEqual(cmd[cmd.index("-start") + 1], "S")
        self.assertEqual(cmd[cmd.index("-end") + 1], "E")
        self.assertIn("-compress", cmd)


class AuthTests(unittest.TestCase):
    def test_no_auth(self):
        self.assertIsNone(parse_basic_or_token_auth(None, None, None))

    def test_token_wins(self):
        self.assertEqual(
            parse_basic_or_token_auth("u", "p", "tok"), "Bearer tok"
        )

    def test_basic(self):
        # base64("admin:secret") == "YWRtaW46c2VjcmV0"
        self.assertEqual(
            parse_basic_or_token_auth("admin", "secret", None),
            "Basic YWRtaW46c2VjcmV0",
        )

    def test_basic_empty_password(self):
        self.assertEqual(
            parse_basic_or_token_auth("admin", None, None),
            "Basic " + "YWRtaW46",  # base64("admin:")
        )


def _fake_runner(lines):
    """A LineRunner seam backed by a fixed list of lines (no subprocess)."""

    def run(_cmd):
        return list(lines)

    return run


class StreamAndDryRunTests(unittest.TestCase):
    def test_dry_run_counts_per_target_table_and_skips_comments(self):
        lines = [
            "# influx_inspect progress marker",
            "",
            "cpu,host=a v=1 5",
            "cpu,host=b v=2 6",
            "net,host=a rx=3i 7",
            "weather\\,sensor,host=a t=20 8",
        ]
        counts = run_bulk_import(
            database="mydb",
            cmd=["x"],
            prefix="mydb_",
            feeder=None,  # dry run
            runner=_fake_runner(lines),
        )
        self.assertEqual(
            counts,
            {"mydb_cpu": 2, "mydb_net": 1, "mydb_weather,sensor": 1},
        )

    def test_measurement_filter_keeps_only_matching_source(self):
        lines = ["cpu,h=a v=1 5", "net,h=a v=2 6", "cpu,h=b v=3 7"]
        counts = run_bulk_import(
            database="mydb",
            cmd=["x"],
            prefix="mydb_",
            feeder=None,
            measurement="cpu",
            runner=_fake_runner(lines),
        )
        self.assertEqual(counts, {"mydb_cpu": 2})

    def test_no_prefix_empty_prefix_keeps_bare_names(self):
        lines = ["cpu,h=a v=1 5"]
        counts = run_bulk_import(
            database="mydb",
            cmd=["x"],
            prefix="",
            feeder=None,
            runner=_fake_runner(lines),
        )
        self.assertEqual(counts, {"cpu": 1})


class _RecordingFeeder:
    """Captures rewritten lines instead of POSTing, to assert what was fed."""

    def __init__(self):
        self.added = []
        self.flushed = False

    def add(self, line):
        self.added.append(line)

    def flush(self):
        self.flushed = True


class FeederIntegrationTests(unittest.TestCase):
    def test_run_feeds_rewritten_lines_and_flushes(self):
        feeder = _RecordingFeeder()
        lines = ["cpu,host=a v=1 5", "net rx=2i 6"]
        run_bulk_import(
            database="mydb",
            cmd=["x"],
            prefix="mydb_",
            feeder=feeder,
            runner=_fake_runner(lines),
        )
        self.assertEqual(
            feeder.added, ["mydb_cpu,host=a v=1 5", "mydb_net rx=2i 6"]
        )
        self.assertTrue(feeder.flushed)


class _FakeResp:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return b""


class IlpHttpFeederBatchingTests(unittest.TestCase):
    """Drive the real _IlpHttpFeeder with a fake urlopen to assert batching and
    the exact LP body it would POST -- no socket involved."""

    def setUp(self):
        self.posts = []  # (url, body, headers)

        def fake_urlopen(req, timeout=None):
            self.posts.append(
                (req.full_url, req.data, dict(req.header_items()))
            )
            return _FakeResp()

        self._orig = bulk_v1.urllib.request.urlopen
        bulk_v1.urllib.request.urlopen = fake_urlopen

    def tearDown(self):
        bulk_v1.urllib.request.urlopen = self._orig

    def test_batches_flush_at_size_and_final_flush(self):
        feeder = bulk_v1._IlpHttpFeeder(
            "http://qdb:9000/", batch_size=2, auth="Bearer t"
        )
        for line in ("a v=1", "b v=2", "c v=3"):
            feeder.add(line)
        # 2 lines triggered an auto-flush; one line still buffered.
        self.assertEqual(len(self.posts), 1)
        feeder.flush()
        self.assertEqual(len(self.posts), 2)
        self.assertEqual(feeder.lines_sent, 3)
        self.assertEqual(feeder.batches_sent, 2)

        url, body, headers = self.posts[0]
        self.assertEqual(url, "http://qdb:9000/write")
        self.assertEqual(body, b"a v=1\nb v=2\n")
        # urllib title-cases header keys.
        self.assertEqual(headers.get("Authorization"), "Bearer t")
        self.assertEqual(self.posts[1][1], b"c v=3\n")

    def test_empty_flush_is_a_noop(self):
        feeder = bulk_v1._IlpHttpFeeder("http://qdb:9000", batch_size=10, auth=None)
        feeder.flush()
        self.assertEqual(self.posts, [])

    def test_no_auth_header_when_unauthenticated(self):
        feeder = bulk_v1._IlpHttpFeeder("http://qdb:9000", batch_size=1, auth=None)
        feeder.add("a v=1")
        _, _, headers = self.posts[0]
        self.assertNotIn("Authorization", headers)


class PlanPrefixTests(unittest.TestCase):
    def test_clean_db_yields_prefix(self):
        self.assertEqual(bulk_v1._plan_prefix("mydb", True), "mydb_")

    def test_no_prefix_yields_empty(self):
        self.assertEqual(bulk_v1._plan_prefix("mydb", False), "")

    def test_unclean_db_rejected(self):
        with self.assertRaises(InvalidScopeName):
            bulk_v1._plan_prefix("my-db", True)


class MainCliTests(unittest.TestCase):
    def test_main_dry_run_rejects_unclean_db(self):
        rc = bulk_v1.main(
            [
                "--database", "my-db",
                "--datadir", "/d", "--waldir", "/w",
                "--questdb-url", "http://localhost:9000",
                "--dry-run",
            ]
        )
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
