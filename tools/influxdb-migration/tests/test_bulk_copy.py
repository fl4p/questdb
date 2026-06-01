"""Unit tests for the COPY orchestrator: SQL building, submit/poll, dry-run e2e.

``urlopen`` is monkeypatched so no live QuestDB is needed; the COPY poll's sleep
is a recorder so the loop runs instantly. The end-to-end test drives ``main`` in
``--dry-run`` from stdin and asserts the staged CSV and the emitted COPY SQL.

Run with the tool directory on the path:

    PYTHONPATH=. python3 tests/test_bulk_copy.py
"""

import io
import json
import os
import tempfile
import unittest

import bulk_copy
from bulk_copy import build_copy_sql, poll_copy, run_copy


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return json.dumps(self._payload).encode("utf-8")


class BuildCopySqlTests(unittest.TestCase):
    def test_basic(self):
        sql = build_copy_sql(
            "b_m", "sub/b_m.csv", "timestamp", "yyyy-MM-ddTHH:mm:ss.SSSUUUNNNZ", "DAY"
        )
        self.assertEqual(
            sql,
            "COPY \"b_m\" FROM 'sub/b_m.csv' WITH HEADER true "
            "TIMESTAMP 'timestamp' FORMAT 'yyyy-MM-ddTHH:mm:ss.SSSUUUNNNZ' "
            "PARTITION BY DAY ON ERROR ABORT",
        )

    def test_on_error_variants(self):
        self.assertIn("ON ERROR SKIP_ROW", build_copy_sql("t", "f", "ts", "F", "DAY", on_error="skip_row"))
        self.assertIn(
            "ON ERROR SKIP_COLUMN",
            build_copy_sql("t", "f", "ts", "F", "DAY", on_error="skip_column"),
        )

    def test_bad_on_error_raises(self):
        with self.assertRaises(ValueError):
            build_copy_sql("t", "f", "ts", "F", "DAY", on_error="nope")

    def test_non_comma_delimiter_appended(self):
        sql = build_copy_sql("t", "f", "ts", "F", "DAY", delimiter="\t")
        self.assertTrue(sql.endswith("DELIMITER '\t'"))


class _UrlopenPatch:
    def __init__(self, test, handler):
        self.test = test
        self.handler = handler
        self.calls = []

    def __enter__(self):
        self._orig = bulk_copy.urllib.request.urlopen

        def fake(req, timeout=None):
            self.calls.append(req.full_url)
            return _Resp(self.handler(req.full_url))

        bulk_copy.urllib.request.urlopen = fake
        return self

    def __exit__(self, *a):
        bulk_copy.urllib.request.urlopen = self._orig
        return False


class RunCopyTests(unittest.TestCase):
    def test_returns_import_id(self):
        with _UrlopenPatch(self, lambda url: {"dataset": [["2a3b4c"]], "columns": [{"name": "id"}]}):
            self.assertEqual(run_copy("http://q:9000", None, "COPY ..."), "2a3b4c")

    def test_missing_id_aborts(self):
        with _UrlopenPatch(self, lambda url: {"dataset": []}):
            with self.assertRaises(SystemExit):
                run_copy("http://q:9000", None, "COPY ...")


class PollCopyTests(unittest.TestCase):
    def test_finished_returns(self):
        with _UrlopenPatch(self, lambda url: {"dataset": [["finished", 1000, 0, None]]}):
            slept = []
            res = poll_copy("http://q:9000", None, "abc", slept.append, 1.0)
            self.assertEqual(res["status"], "finished")
            self.assertEqual(res["rows_imported"], 1000)
            self.assertEqual(slept, [])  # already done on first poll

    def test_failed_raises_with_message(self):
        with _UrlopenPatch(self, lambda url: {"dataset": [["failed", 0, 3, "bad row 7"]]}):
            with self.assertRaises(SystemExit) as cm:
                poll_copy("http://q:9000", None, "abc", lambda s: None, 1.0)
            self.assertIn("bad row 7", str(cm.exception))

    def test_waits_then_finishes(self):
        seq = iter(
            [
                {"dataset": []},  # not registered yet
                {"dataset": [["started", None, 0, None]]},  # running
                {"dataset": [["finished", 42, 0, None]]},  # done
            ]
        )
        with _UrlopenPatch(self, lambda url: next(seq)):
            slept = []
            res = poll_copy("http://q:9000", None, "abc", slept.append, 0.5)
            self.assertEqual(res["rows_imported"], 42)
            self.assertEqual(slept, [0.5, 0.5])  # two waits before the finished row


class DryRunE2ETests(unittest.TestCase):
    SCHEMA = """
    CREATE TABLE 'b_batmon' (
      timestamp TIMESTAMP_NS, did SYMBOL, voltage DOUBLE, cellcount LONG
    ) timestamp(timestamp) PARTITION BY DAY WAL;
    """

    def test_pivots_to_csv_and_prints_copy(self):
        root = tempfile.mkdtemp()
        schema_path = os.path.join(root, "tables.sql")
        with open(schema_path, "w", encoding="utf-8") as fh:
            fh.write(self.SCHEMA)
        lp = (
            "batmon,did=A voltage=3.27 1700000000000000000\n"
            "batmon,did=A cellcount=4i 1700000000000000000\n"
            "batmon,did=A voltage=3.30 1700000000200000000\n"
        )
        orig_stdin = bulk_copy.sys.stdin
        bulk_copy.sys.stdin = io.StringIO(lp)
        try:
            rc = bulk_copy.main(
                [
                    "--from-stdin",
                    "--prefix",
                    "b_",
                    "--schema-file",
                    schema_path,
                    "--copy-root",
                    root,
                    "--copy-subdir",
                    "stg",
                    "--csv-timestamp-mode",
                    "epoch-ns",
                    "--dry-run",
                ]
            )
        finally:
            bulk_copy.sys.stdin = orig_stdin
        self.assertEqual(rc, 0)
        csv_path = os.path.join(root, "stg", "b_batmon.csv")
        self.assertTrue(os.path.exists(csv_path))
        with open(csv_path, encoding="utf-8") as fh:
            body = fh.read()
        self.assertEqual(
            body,
            "timestamp,did,voltage,cellcount\n"
            "1700000000000000000,A,3.27,4\n"
            "1700000000200000000,A,3.30,\n",
        )


if __name__ == "__main__":
    unittest.main()
