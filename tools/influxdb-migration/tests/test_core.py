"""Unit tests for the dependency-free core: naming, ACL mapping, coercion.

Run with the tool directory on the path:

    PYTHONPATH=.. python3 test_core.py
    # or, from the tool dir:  python3 -m unittest discover tests
"""

import unittest

from acl import AclNameCollision, build_entries, render
from model import (
    Access,
    FieldType,
    Grant,
    InvalidScopeName,
    Principal,
    scope_prefix,
    table_name,
    validate_scope,
)
from writer import _coerce


class NamingTests(unittest.TestCase):
    def test_validate_accepts_clean_identifier(self):
        validate_scope("ok_name9")  # must not raise

    def test_validate_rejects_unclean_names(self):
        for bad in ("my-db", "my.db", "my db", ""):
            with self.assertRaises(InvalidScopeName):
                validate_scope(bad)

    def test_table_name_single_underscore_separator(self):
        # The /query endpoint strips the exact "<db>_" prefix, so the separator
        # must be a single underscore and measurement underscores must survive.
        self.assertEqual(
            table_name("{db}_{measurement}", "mydb", "disk_free", True),
            "mydb_disk_free",
        )

    def test_table_name_no_prefix(self):
        self.assertEqual(
            table_name("{db}_{measurement}", "mydb", "cpu", False), "cpu"
        )

    def test_scope_prefix(self):
        self.assertEqual(scope_prefix("mydb"), "mydb_")


class AclTests(unittest.TestCase):
    def _by_name(self, entries):
        return {e.name: e for e in entries}

    def test_admin_gets_rw_no_prefix(self):
        entries = build_entries([Principal("root", is_admin=True)], "widest")
        e = entries[0]
        self.assertEqual(e.access, Access.RW)
        self.assertEqual(e.prefix, "")

    def test_single_grant_maps_to_prefix_and_access(self):
        p = Principal("alice", grants=[Grant("projectA", Access.RO)])
        e = build_entries([p], "widest")[0]
        self.assertEqual(e.access, Access.RO)
        self.assertEqual(e.prefix, "projectA_")

    def test_no_grant_user_is_locked_down(self):
        p = Principal("ghost", is_admin=False, grants=[])
        e = build_entries([p], "widest")[0]
        self.assertEqual(e.access, Access.RO)
        self.assertEqual(e.prefix, "__noaccess__")  # cannot match any real table

    def test_multi_scope_widest_drops_prefix_least_privilege(self):
        p = Principal("bob", grants=[Grant("a", Access.RW), Grant("b", Access.RO)])
        e = build_entries([p], "widest")[0]
        self.assertEqual(e.prefix, "")
        self.assertEqual(e.access, Access.RO)  # not all grants are rw

    def test_multi_scope_widest_rw_when_all_rw(self):
        p = Principal("bob", grants=[Grant("a", Access.RW), Grant("b", Access.RW)])
        e = build_entries([p], "widest")[0]
        self.assertEqual(e.access, Access.RW)

    def test_multi_scope_skip_omits_user(self):
        p = Principal("bob", grants=[Grant("a", Access.RW), Grant("b", Access.RO)])
        self.assertEqual(build_entries([p], "skip"), [])

    def test_multi_scope_split_note_keeps_first(self):
        p = Principal("bob", grants=[Grant("a", Access.RW), Grant("b", Access.RO)])
        e = build_entries([p], "split-note")[0]
        self.assertEqual(e.prefix, "a_")
        self.assertEqual(e.access, Access.RW)

    def test_unsafe_username_is_sanitized(self):
        p = Principal("a.b c", is_admin=True)
        e = build_entries([p], "widest")[0]
        self.assertEqual(e.name, "a_b_c")

    def test_colliding_usernames_hard_fail(self):
        # 'alice.x' and 'alice-x' both reduce to acl key 'alice_x'.
        principals = [
            Principal("alice.x", grants=[Grant("a", Access.RO)]),
            Principal("alice-x", grants=[Grant("b", Access.RW)]),
        ]
        with self.assertRaises(AclNameCollision):
            build_entries(principals, "widest")

    def test_no_prefix_single_grant_gets_empty_prefix(self):
        # Under --no-prefix tables are bare, so a <db>_ prefix would lock the
        # user out; the grant must map to all-tables (empty prefix).
        p = Principal("alice", grants=[Grant("onlydb", Access.RW)])
        e = build_entries([p], "widest", use_prefix=False)[0]
        self.assertEqual(e.prefix, "")
        self.assertEqual(e.access, Access.RW)

    def test_password_map_reused(self):
        p = Principal("alice", grants=[Grant("a", Access.RW)])
        e = build_entries([p], "widest", {"alice": "known"})[0]
        self.assertEqual(e.password, "known")

    def test_render_format(self):
        p = Principal("alice", grants=[Grant("a", Access.RO)])
        entries = build_entries([p], "widest", {"alice": "pw"})
        text = render(entries)
        self.assertIn("user.alice.password=pw", text)
        self.assertIn("user.alice.access=ro", text)
        self.assertIn("user.alice.prefix=a_", text)

    def test_render_omits_empty_prefix(self):
        entries = build_entries([Principal("root", is_admin=True)], "widest")
        text = render(entries)
        self.assertNotIn(".prefix=", text)


class CoercionTests(unittest.TestCase):
    def test_basic_types(self):
        self.assertEqual(_coerce(1, FieldType.INTEGER), 1)
        self.assertEqual(_coerce("1", FieldType.INTEGER), 1)
        self.assertEqual(_coerce(1.5, FieldType.FLOAT), 1.5)
        self.assertEqual(_coerce("x", FieldType.STRING), "x")
        self.assertIs(_coerce(0, FieldType.BOOLEAN), False)

    def test_null_skipped(self):
        self.assertIsNone(_coerce(None, FieldType.FLOAT))

    def test_bad_value_skipped(self):
        self.assertIsNone(_coerce("not-a-number", FieldType.INTEGER))

    def test_untyped_passthrough(self):
        self.assertEqual(_coerce(3, None), 3)
        self.assertIs(_coerce(True, None), True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
