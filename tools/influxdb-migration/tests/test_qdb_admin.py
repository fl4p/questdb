"""Unit tests for qdb_admin: index-spec parsing, CREATE TABLE DDL, ensure call.

The pure builders are tested directly; ``ensure_indexed_table``'s ``urlopen`` is
monkeypatched to a recorder so no live QuestDB is needed.

Run with the tool directory on the path:

    PYTHONPATH=. python3 tests/test_qdb_admin.py
"""

import unittest

import json

import qdb_admin
from qdb_admin import (
    IndexSpecError,
    build_create_table_ddl,
    build_full_create_table_ddl,
    ensure_indexed_table,
    parse_index_spec,
    parse_schema_columns,
    parse_schema_tables,
)


class _FakeResp:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return b'{"ddl":"OK"}'


class _FakeJsonResp:
    def __init__(self, payload):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return json.dumps(self._payload).encode("utf-8")


class ParseIndexSpecTests(unittest.TestCase):
    def test_empty_and_none_yield_no_specs(self):
        self.assertEqual(parse_index_spec(""), [])
        self.assertEqual(parse_index_spec(None), [])
        self.assertEqual(parse_index_spec("  ,  , "), [])

    def test_plain_columns(self):
        self.assertEqual(
            parse_index_spec("did,uid,addrh"),
            [("did", None), ("uid", None), ("addrh", None)],
        )

    def test_capacity(self):
        self.assertEqual(
            parse_index_spec("addrh:2048, slug:64"),
            [("addrh", 2048), ("slug", 64)],
        )

    def test_order_preserved_and_whitespace_trimmed(self):
        self.assertEqual(
            parse_index_spec(" uid , did:128 "),
            [("uid", None), ("did", 128)],
        )

    def test_non_integer_capacity_rejected(self):
        with self.assertRaises(IndexSpecError):
            parse_index_spec("addrh:big")

    def test_non_positive_capacity_rejected(self):
        with self.assertRaises(IndexSpecError):
            parse_index_spec("addrh:0")
        with self.assertRaises(IndexSpecError):
            parse_index_spec("addrh:-5")

    def test_duplicate_column_rejected(self):
        with self.assertRaises(IndexSpecError):
            parse_index_spec("did,did")

    def test_bad_identifier_rejected(self):
        for bad in ("1col", "a-b", "a b", "a;b", ":128"):
            with self.assertRaises(IndexSpecError):
                parse_index_spec(bad)


class BuildDdlTests(unittest.TestCase):
    def test_bare_table_no_index(self):
        ddl = build_create_table_ddl("t", [])
        self.assertEqual(
            ddl,
            "CREATE TABLE IF NOT EXISTS 't' (\n"
            "    timestamp TIMESTAMP\n"
            ") timestamp(timestamp) PARTITION BY DAY WAL",
        )

    def test_with_indexes_and_capacity(self):
        ddl = build_create_table_ddl(
            "batmon_tele_batmon", [("did", None), ("addrh", 2048)]
        )
        self.assertIn("    did SYMBOL INDEX,", ddl)
        self.assertIn("    addrh SYMBOL INDEX CAPACITY 2048\n", ddl)
        self.assertTrue(ddl.startswith("CREATE TABLE IF NOT EXISTS 'batmon_tele_batmon' ("))
        self.assertIn("timestamp(timestamp) PARTITION BY DAY WAL", ddl)

    def test_nanosecond_timestamp_and_no_wal_and_partition(self):
        ddl = build_create_table_ddl(
            "t", [("did", None)], timestamp_type="timestamp_ns",
            partition_by="HOUR", wal=False,
        )
        self.assertIn("    timestamp TIMESTAMP_NS,", ddl)
        self.assertIn("PARTITION BY HOUR", ddl)
        self.assertNotIn("WAL", ddl)

    def test_invalid_timestamp_type_rejected(self):
        with self.assertRaises(IndexSpecError):
            build_create_table_ddl("t", [], timestamp_type="TIMESTAMP_MS")

    def test_empty_table_rejected(self):
        with self.assertRaises(IndexSpecError):
            build_create_table_ddl("   ", [])


class EnsureIndexedTableTests(unittest.TestCase):
    def setUp(self):
        self.calls = []

        def fake_urlopen(req, timeout=None):
            self.calls.append((req.full_url, dict(req.header_items())))
            return _FakeResp()

        self._orig = qdb_admin.urllib.request.urlopen
        qdb_admin.urllib.request.urlopen = fake_urlopen
        self.addCleanup(
            lambda: setattr(qdb_admin.urllib.request, "urlopen", self._orig)
        )

    def test_posts_create_to_exec_with_auth(self):
        ddl = ensure_indexed_table(
            "http://qdb:9000/", "Bearer t", "tbl", [("did", None)]
        )
        self.assertEqual(len(self.calls), 1)
        url, headers = self.calls[0]
        self.assertTrue(url.startswith("http://qdb:9000/exec?query="))
        # the DDL we ran is returned and reflects the index
        self.assertIn("did SYMBOL INDEX", ddl)
        self.assertEqual(headers.get("Authorization"), "Bearer t")
        # url-encoded DDL contains the table name and CREATE IF NOT EXISTS
        self.assertIn("CREATE+TABLE+IF+NOT+EXISTS", url)
        self.assertIn("tbl", url)

    def test_no_auth_header_when_unauthenticated(self):
        ensure_indexed_table("http://qdb:9000", None, "tbl", [])
        _, headers = self.calls[0]
        self.assertNotIn("Authorization", headers)

    def test_http_error_aborts(self):
        import urllib.error
        import io

        def boom(req, timeout=None):
            raise urllib.error.HTTPError(
                req.full_url, 400, "Bad Request", {}, io.BytesIO(b"nope")
            )

        qdb_admin.urllib.request.urlopen = boom
        with self.assertRaises(SystemExit):
            ensure_indexed_table("http://qdb:9000", None, "tbl", [])


class ParseSchemaColumnsTests(unittest.TestCase):
    SCHEMA = """
    -- a comment line, and an inline -- note
    CREATE TABLE 'batmon_tele_batmon' (
        timestamp TIMESTAMP,
        device SYMBOL,
        did SYMBOL INDEX,
        voltage FLOAT,
        voltage_cell000 INT,
        problem_code LONG,
        switches_charge BOOLEAN
    ) timestamp(timestamp) PARTITION BY DAY;

    CREATE TABLE 'batmon_tele_cells' (
        timestamp TIMESTAMP,
        cell_index SYMBOL,
        voltage INT
    ) timestamp(timestamp) PARTITION BY DAY;
    """

    def test_tables_and_kinds(self):
        tables = parse_schema_columns(self.SCHEMA)
        self.assertEqual(set(tables), {"batmon_tele_batmon", "batmon_tele_cells"})
        b = tables["batmon_tele_batmon"]
        self.assertEqual(b["device"], "str")
        self.assertEqual(b["did"], "str")  # SYMBOL INDEX -> str (index ignored)
        self.assertEqual(b["voltage"], "float")
        self.assertEqual(b["voltage_cell000"], "int")
        self.assertEqual(b["problem_code"], "int")  # LONG -> int kind
        self.assertEqual(b["switches_charge"], "bool")

    def test_designated_timestamp_excluded(self):
        b = parse_schema_columns(self.SCHEMA)["batmon_tele_batmon"]
        self.assertNotIn("timestamp", b)

    def test_cells(self):
        c = parse_schema_columns(self.SCHEMA)["batmon_tele_cells"]
        self.assertEqual(c, {"cell_index": "str", "voltage": "int"})

    def test_if_not_exists_and_unquoted_name(self):
        tables = parse_schema_columns(
            "CREATE TABLE IF NOT EXISTS t (timestamp TIMESTAMP, x INT) "
            "timestamp(timestamp) PARTITION BY DAY"
        )
        self.assertEqual(tables, {"t": {"x": "int"}})

    def test_empty_input_yields_no_tables(self):
        self.assertEqual(parse_schema_columns("-- just a comment\n"), {})


class ParseSchemaTablesTests(unittest.TestCase):
    SCHEMA = """
    CREATE TABLE 'batmon_tele_batmon' (
        timestamp TIMESTAMP_NS,
        did SYMBOL INDEX CAPACITY 2048,
        uid SYMBOL,
        voltage DOUBLE,
        problem_code LONG,
        charging BOOLEAN
    ) timestamp(timestamp) PARTITION BY HOUR;
    """

    def test_order_types_and_flags(self):
        s = parse_schema_tables(self.SCHEMA)["batmon_tele_batmon"]
        self.assertEqual(
            [c.name for c in s.columns],
            ["timestamp", "did", "uid", "voltage", "problem_code", "charging"],
        )
        self.assertEqual(s.timestamp_col, "timestamp")
        self.assertEqual(s.timestamp_type, "TIMESTAMP_NS")
        self.assertEqual(s.partition_by, "HOUR")
        self.assertEqual(s.symbol_columns(), ["did", "uid"])
        self.assertEqual(
            s.field_columns(), ["voltage", "problem_code", "charging"]
        )
        did = next(c for c in s.columns if c.name == "did")
        self.assertTrue(did.is_symbol)
        self.assertTrue(did.indexed)
        self.assertEqual(did.definition, "SYMBOL INDEX CAPACITY 2048")

    def test_partition_by_absent_is_none(self):
        s = parse_schema_tables(
            "CREATE TABLE t (timestamp TIMESTAMP, x INT) timestamp(timestamp)"
        )["t"]
        self.assertIsNone(s.partition_by)


class BuildFullDdlTests(unittest.TestCase):
    def _schema(self):
        return parse_schema_tables(
            "CREATE TABLE 'b_m' ("
            "timestamp TIMESTAMP, did SYMBOL INDEX, v DOUBLE, n LONG"
            ") timestamp(timestamp) PARTITION BY DAY"
        )["b_m"]

    def test_full_column_list_and_dedup(self):
        ddl = build_full_create_table_ddl(self._schema(), timestamp_type="TIMESTAMP_NS")
        self.assertIn("timestamp TIMESTAMP_NS", ddl)
        self.assertIn("did SYMBOL INDEX", ddl)
        self.assertIn("v DOUBLE", ddl)
        self.assertIn("n LONG", ddl)
        self.assertIn("PARTITION BY DAY", ddl)
        self.assertTrue(ddl.rstrip().endswith("DEDUP UPSERT KEYS(timestamp, did)"))

    def test_dedup_off(self):
        ddl = build_full_create_table_ddl(self._schema(), dedup=False)
        self.assertNotIn("DEDUP", ddl)

    def test_partition_override_required_when_absent(self):
        s = parse_schema_tables(
            "CREATE TABLE t (timestamp TIMESTAMP, x INT) timestamp(timestamp)"
        )["t"]
        with self.assertRaises(IndexSpecError):
            build_full_create_table_ddl(s)  # no PARTITION BY in DDL, none overridden
        ddl = build_full_create_table_ddl(s, partition_by="MONTH")
        self.assertIn("PARTITION BY MONTH", ddl)

    def test_bad_timestamp_type_rejected(self):
        with self.assertRaises(IndexSpecError):
            build_full_create_table_ddl(self._schema(), timestamp_type="NANOS")


class FetchTableSchemaTests(unittest.TestCase):
    def _patch(self, payload=None, error=None):
        def fake_urlopen(req, timeout=None):
            if error is not None:
                raise error
            return _FakeJsonResp(payload)

        self._orig = qdb_admin.urllib.request.urlopen
        qdb_admin.urllib.request.urlopen = fake_urlopen
        self.addCleanup(
            lambda: setattr(qdb_admin.urllib.request, "urlopen", self._orig)
        )

    def test_builds_schema_from_show_columns(self):
        self._patch(
            {
                "columns": [
                    {"name": "column"},
                    {"name": "type"},
                    {"name": "indexed"},
                    {"name": "designated"},
                ],
                "dataset": [
                    ["timestamp", "TIMESTAMP", False, True],
                    ["did", "SYMBOL", True, False],
                    ["voltage", "DOUBLE", False, False],
                ],
            }
        )
        s = qdb_admin.fetch_table_schema("http://qdb:9000", None, "b_m")
        self.assertIsNotNone(s)
        self.assertEqual(s.timestamp_col, "timestamp")
        self.assertEqual(s.symbol_columns(), ["did"])
        self.assertEqual(s.field_columns(), ["voltage"])
        did = next(c for c in s.columns if c.name == "did")
        self.assertEqual(did.definition, "SYMBOL INDEX")

    def test_missing_table_returns_none(self):
        import io
        import urllib.error

        self._patch(
            error=urllib.error.HTTPError(
                "http://qdb:9000/exec",
                400,
                "Bad Request",
                {},
                io.BytesIO(b"table does not exist"),
            )
        )
        self.assertIsNone(
            qdb_admin.fetch_table_schema("http://qdb:9000", None, "nope")
        )

    def test_other_http_error_aborts(self):
        import io
        import urllib.error

        self._patch(
            error=urllib.error.HTTPError(
                "http://qdb:9000/exec",
                500,
                "Server Error",
                {},
                io.BytesIO(b"boom"),
            )
        )
        with self.assertRaises(SystemExit):
            qdb_admin.fetch_table_schema("http://qdb:9000", None, "b_m")


if __name__ == "__main__":
    unittest.main()
