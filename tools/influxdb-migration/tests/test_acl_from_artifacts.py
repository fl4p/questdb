"""Unit tests for the canonical acl.conf generator (acl_from_artifacts.py).

Run with the tool directory on the path:

    PYTHONPATH=.. python3 -m unittest test_acl_from_artifacts
    # or, from the tool dir:  python3 -m unittest discover tests
"""

import json
import os
import tempfile
import unittest

from acl_from_artifacts import (
    Access,
    AclNameCollision,
    Grant,
    MalformedArtifact,
    Principal,
    build_entries,
    load_manifest,
    load_password_map,
    load_principals,
    render,
    render_credentials_csv,
)


def _entries_by_name(entries):
    return {e.name: e for e in entries}


class ManifestLoadingTests(unittest.TestCase):
    def _write(self, obj):
        fd, path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
        self.addCleanup(os.remove, path)
        return path

    def test_loads_db_to_prefix_map(self):
        path = self._write(
            [
                {"influx_db": "mydb", "prefix": "mydb_", "tables": ["mydb_cpu"]},
                {"influx_db": "other", "prefix": "other_", "tables": []},
            ]
        )
        self.assertEqual(load_manifest(path), {"mydb": "mydb_", "other": "other_"})

    def test_empty_prefix_preserved_for_no_prefix_runs(self):
        # --no-prefix migrations write bare tables; the manifest carries "".
        path = self._write([{"influx_db": "solo", "prefix": "", "tables": ["cpu"]}])
        self.assertEqual(load_manifest(path), {"solo": ""})

    def test_full_schema_with_prefixed_flag(self):
        # The migration tool emits an explicit "prefixed" bool alongside "prefix".
        path = self._write(
            [
                {"influx_db": "mydb", "prefixed": True, "prefix": "mydb_", "tables": ["mydb_cpu"]},
                {"influx_db": "solo", "prefixed": False, "prefix": "", "tables": ["cpu"]},
            ]
        )
        self.assertEqual(load_manifest(path), {"mydb": "mydb_", "solo": ""})

    def test_inconsistent_prefixed_flag_is_rejected(self):
        for entry in (
            {"influx_db": "d", "prefixed": True, "prefix": ""},
            {"influx_db": "d", "prefixed": False, "prefix": "d_"},
        ):
            path = self._write([entry])
            with self.assertRaises(MalformedArtifact):
                load_manifest(path)

    def test_conflicting_prefix_for_same_db_is_rejected(self):
        path = self._write(
            [
                {"influx_db": "d", "prefix": "d_", "tables": []},
                {"influx_db": "d", "prefix": "x_", "tables": []},
            ]
        )
        with self.assertRaises(MalformedArtifact):
            load_manifest(path)

    def test_missing_required_field_is_rejected(self):
        path = self._write([{"influx_db": "d"}])
        with self.assertRaises(MalformedArtifact):
            load_manifest(path)

    def test_non_array_is_rejected(self):
        path = self._write({"influx_db": "d", "prefix": "d_"})
        with self.assertRaises(MalformedArtifact):
            load_manifest(path)

    def test_missing_file_is_rejected(self):
        with self.assertRaises(MalformedArtifact):
            load_manifest("/nonexistent/path/manifest.json")


class PrincipalsLoadingTests(unittest.TestCase):
    def _write(self, obj):
        fd, path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
        self.addCleanup(os.remove, path)
        return path

    def test_loads_admin_and_grants(self):
        path = self._write(
            [
                {"name": "root", "is_admin": True, "grants": []},
                {
                    "name": "reader",
                    "is_admin": False,
                    "grants": [{"scope": "mydb", "access": "ro"}],
                },
            ]
        )
        principals = load_principals(path)
        self.assertEqual(len(principals), 2)
        self.assertTrue(principals[0].is_admin)
        self.assertEqual(principals[1].grants[0].scope, "mydb")
        self.assertEqual(principals[1].grants[0].access, Access.RO)

    def test_missing_grants_defaults_to_empty(self):
        path = self._write([{"name": "u"}])
        principals = load_principals(path)
        self.assertEqual(principals[0].grants, [])
        self.assertFalse(principals[0].is_admin)

    def test_bad_access_value_is_rejected(self):
        path = self._write([{"name": "u", "grants": [{"scope": "d", "access": "admin"}]}])
        with self.assertRaises(MalformedArtifact):
            load_principals(path)

    def test_empty_name_is_rejected(self):
        path = self._write([{"name": "", "grants": []}])
        with self.assertRaises(MalformedArtifact):
            load_principals(path)

    def test_grant_missing_field_is_rejected(self):
        path = self._write([{"name": "u", "grants": [{"scope": "d"}]}])
        with self.assertRaises(MalformedArtifact):
            load_principals(path)


class BuildEntriesTests(unittest.TestCase):
    PREFIXES = {"mydb": "mydb_", "other": "other_"}

    def test_admin_gets_rw_and_no_prefix(self):
        entries = build_entries(
            [Principal("root", is_admin=True)], self.PREFIXES, "widest"
        )
        e = _entries_by_name(entries)["root"]
        self.assertEqual(e.access, Access.RW)
        self.assertEqual(e.prefix, "")

    def test_admin_ignores_grants(self):
        entries = build_entries(
            [Principal("root", is_admin=True, grants=[Grant("mydb", Access.RO)])],
            self.PREFIXES,
            "widest",
        )
        self.assertEqual(_entries_by_name(entries)["root"].prefix, "")

    def test_single_grant_uses_manifest_prefix_verbatim(self):
        entries = build_entries(
            [Principal("r", grants=[Grant("mydb", Access.RO)])], self.PREFIXES, "widest"
        )
        e = _entries_by_name(entries)["r"]
        self.assertEqual(e.access, Access.RO)
        self.assertEqual(e.prefix, "mydb_")

    def test_no_prefix_run_yields_empty_prefix_not_lockout(self):
        # Manifest prefix "" (a --no-prefix migration) must flow through as "".
        entries = build_entries(
            [Principal("r", grants=[Grant("solo", Access.RW)])], {"solo": ""}, "widest"
        )
        self.assertEqual(_entries_by_name(entries)["r"].prefix, "")

    def test_no_grants_gets_locked_down(self):
        entries = build_entries([Principal("nobody")], self.PREFIXES, "widest")
        e = _entries_by_name(entries)["nobody"]
        self.assertEqual(e.access, Access.RO)
        self.assertEqual(e.prefix, "__noaccess__")

    def test_grant_on_unmigrated_scope_is_dropped(self):
        # 'ghost' was not migrated (absent from the manifest) -> grant dropped ->
        # principal has no usable grant -> locked down.
        entries = build_entries(
            [Principal("r", grants=[Grant("ghost", Access.RW)])], self.PREFIXES, "widest"
        )
        self.assertEqual(_entries_by_name(entries)["r"].prefix, "__noaccess__")

    def test_partial_unmigrated_scope_keeps_the_migrated_one(self):
        entries = build_entries(
            [Principal("r", grants=[Grant("ghost", Access.RW), Grant("mydb", Access.RO)])],
            self.PREFIXES,
            "widest",
        )
        e = _entries_by_name(entries)["r"]
        self.assertEqual(e.prefix, "mydb_")
        self.assertEqual(e.access, Access.RO)

    def test_multi_scope_widest_drops_prefix_least_privilege(self):
        entries = build_entries(
            [Principal("r", grants=[Grant("mydb", Access.RW), Grant("other", Access.RO)])],
            self.PREFIXES,
            "widest",
        )
        e = _entries_by_name(entries)["r"]
        self.assertEqual(e.prefix, "")
        self.assertEqual(e.access, Access.RO)  # not all rw -> ro

    def test_multi_scope_widest_rw_only_when_all_rw(self):
        entries = build_entries(
            [Principal("r", grants=[Grant("mydb", Access.RW), Grant("other", Access.RW)])],
            self.PREFIXES,
            "widest",
        )
        self.assertEqual(_entries_by_name(entries)["r"].access, Access.RW)

    def test_multi_scope_skip_omits_user(self):
        entries = build_entries(
            [Principal("r", grants=[Grant("mydb", Access.RW), Grant("other", Access.RO)])],
            self.PREFIXES,
            "skip",
        )
        self.assertNotIn("r", _entries_by_name(entries))

    def test_multi_scope_split_note_keeps_first(self):
        entries = build_entries(
            [Principal("r", grants=[Grant("mydb", Access.RW), Grant("other", Access.RO)])],
            self.PREFIXES,
            "split-note",
        )
        e = _entries_by_name(entries)["r"]
        self.assertEqual(e.prefix, "mydb_")
        self.assertEqual(e.access, Access.RW)

    def test_password_map_is_reused(self):
        entries = build_entries(
            [Principal("r", grants=[Grant("mydb", Access.RO)])],
            self.PREFIXES,
            "widest",
            password_map={"r": "known-pw"},
        )
        self.assertEqual(_entries_by_name(entries)["r"].password, "known-pw")

    def test_generated_password_is_nonempty_and_safe(self):
        entries = build_entries(
            [Principal("r", grants=[Grant("mydb", Access.RO)])], self.PREFIXES, "widest"
        )
        pw = _entries_by_name(entries)["r"].password
        self.assertGreaterEqual(len(pw), 16)
        self.assertTrue(all(c.isalnum() for c in pw))

    def test_username_collision_hard_fails(self):
        with self.assertRaises(AclNameCollision):
            build_entries(
                [
                    Principal("alice.x", grants=[Grant("mydb", Access.RO)]),
                    Principal("alice-x", grants=[Grant("mydb", Access.RO)]),
                ],
                self.PREFIXES,
                "widest",
            )

    def test_same_name_repeated_is_not_a_collision(self):
        # Identical names are deduped by the collision check, not a hard error.
        entries = build_entries(
            [
                Principal("dup", grants=[Grant("mydb", Access.RO)]),
                Principal("dup", grants=[Grant("mydb", Access.RO)]),
            ],
            self.PREFIXES,
            "widest",
        )
        self.assertEqual(len(entries), 2)


class RenderTests(unittest.TestCase):
    PREFIXES = {"mydb": "mydb_"}

    def test_acl_conf_format(self):
        entries = build_entries(
            [
                Principal("root", is_admin=True),
                Principal("reader", grants=[Grant("mydb", Access.RO)]),
            ],
            self.PREFIXES,
            "widest",
            password_map={"root": "p1", "reader": "p2"},
        )
        text = render(entries)
        self.assertIn("user.root.password=p1", text)
        self.assertIn("user.root.access=rw", text)
        # admin has empty prefix -> no prefix line emitted
        self.assertNotIn("user.root.prefix=", text)
        self.assertIn("user.reader.access=ro", text)
        self.assertIn("user.reader.prefix=mydb_", text)

    def test_no_access_prefix_is_rendered(self):
        entries = build_entries([Principal("nobody")], self.PREFIXES, "widest")
        self.assertIn("user.nobody.prefix=__noaccess__", render(entries))

    def test_locked_down_user_still_has_a_password(self):
        # The fork's ACL loader hard-fails server startup if any user block lacks
        # a password, so even a locked-down/no-grant user must carry a password
        # line. Guard that interop contract here.
        entries = build_entries([Principal("nobody")], self.PREFIXES, "widest")
        self.assertTrue(_entries_by_name(entries)["nobody"].password)
        self.assertIn("user.nobody.password=", render(entries))

    def test_credentials_csv(self):
        entries = build_entries(
            [Principal("reader", grants=[Grant("mydb", Access.RO)])],
            self.PREFIXES,
            "widest",
            password_map={"reader": "secret"},
        )
        csv_text = render_credentials_csv(entries)
        self.assertEqual(csv_text.splitlines()[0], "username,password")
        self.assertIn("reader,secret", csv_text)


class PasswordMapTests(unittest.TestCase):
    def test_loads_and_ignores_comments_and_blanks(self):
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write("# comment\n\nalice,pw1\nbob, pw2 \n")
        self.addCleanup(os.remove, path)
        mapping = load_password_map(path)
        self.assertEqual(mapping, {"alice": "pw1", "bob": "pw2"})

    def test_empty_path_returns_empty_map(self):
        self.assertEqual(load_password_map(None), {})


if __name__ == "__main__":
    unittest.main()
