#!/usr/bin/env python3
"""Canonical generator for the fork's file-based ACL (conf/acl.conf).

This is the single source of truth for acl.conf. It consumes the two artifacts
the migration tool (influx_migrate.py) emits on every run and replays InfluxDB
users/permissions onto the fork's file-based ACL (see qdb-inf/docs/ACL.md):

    migration-manifest.json : [{"influx_db": str, "prefixed": bool,
                                "prefix": str, "tables": [str]}]
    principals.json         : [{"name": str, "is_admin": bool,
                                "grants": [{"scope": str, "access": "ro"|"rw"}]}]

``prefixed`` is false with an empty ``prefix`` for ``--no-prefix`` runs, and true
with ``prefix="<db>_"`` otherwise. This generator consumes ``prefix`` verbatim
(the empty string already yields an empty acl.conf prefix), and cross-checks
``prefixed`` against it to catch a malformed manifest.

Decoupling ACL generation from the data migration lets an operator regenerate or
adjust the ACL later -- rotate passwords, switch ``--multi-scope-policy`` -- WITHOUT
re-reading InfluxDB. ``influx_migrate.py`` keeps an opt-in ``--emit-acl-conf`` path
for convenience, but this tool produces the canonical file.

Why consume artifacts instead of recomputing prefixes: each principal's table
prefix is taken VERBATIM from the manifest entry for its scope, so the acl.conf
prefix is guaranteed identical to the table prefix the migration actually wrote
-- including ``--no-prefix`` runs, where the manifest prefix is the empty string.
This generator never re-derives or sanitizes a prefix, so it cannot drift from
the data path.

InfluxDB never exposes existing passwords (v1 stores hashes, v2 uses tokens), so
this tool reuses an operator-supplied password map or generates strong random
passwords and writes a credentials CSV for redistribution.

acl.conf expresses ONE prefix + ONE access per username. A principal holding
grants on several scopes is resolved by ``--multi-scope-policy`` (widest | skip |
split-note), always logging the choice.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import secrets
import string
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("acl_from_artifacts")

_PW_ALPHABET = string.ascii_letters + string.digits
# acl.conf keys are dotted (user.<name>.password); anything outside [A-Za-z0-9_]
# (dots, dashes, '=', whitespace) could break key parsing, so reduce names to
# that charset and hard-fail if two distinct names collide.
_SAFE_NAME_CHARS = set(string.ascii_letters + string.digits + "_")
# Prefix pinned onto a principal that ends up with no authorizable scope. Real
# prefixes are "<db>_" (a db named "__noaccess__" would yield "__noaccess___",
# with a trailing underscore), so a user bound here matches no migrated table.
_NO_ACCESS_PREFIX = "__noaccess__"


class Access(str, Enum):
    """Effective access level for an ACL principal grant."""

    RO = "ro"
    RW = "rw"


class AclNameCollision(ValueError):
    """Two distinct InfluxDB usernames reduce to the same acl.conf key.

    Emitting both would produce duplicate ``user.<name>.*`` keys with different
    passwords/access -- undefined loader behavior -- so we hard-fail and name the
    colliding users rather than silently clobbering one.
    """


class MalformedArtifact(ValueError):
    """An input artifact is missing required fields or carries bad values."""


@dataclass
class Grant:
    """One principal's access to one scope (db for v1, bucket for v2)."""

    scope: str
    access: Access


@dataclass
class Principal:
    """A user/token to replay into acl.conf.

    ``is_admin`` principals get full access with no prefix. Non-admin principals
    carry zero or more :class:`Grant` entries.
    """

    name: str
    is_admin: bool = False
    grants: List[Grant] = field(default_factory=list)


@dataclass
class AclEntry:
    name: str
    password: str
    access: Access
    prefix: str  # "" means all tables (no restriction)


def load_manifest(path: str) -> Dict[str, str]:
    """Read migration-manifest.json into a ``{influx_db: prefix}`` map.

    The prefix is taken verbatim; this generator never recomputes it. Raises
    :class:`MalformedArtifact` on a structurally invalid file or a db that
    appears twice with conflicting prefixes.
    """
    data = _load_json(path)
    if not isinstance(data, list):
        raise MalformedArtifact(f"{path}: expected a JSON array of manifest entries")
    mapping: Dict[str, str] = {}
    for i, entry in enumerate(data):
        if not isinstance(entry, dict):
            raise MalformedArtifact(f"{path}[{i}]: expected an object")
        if "influx_db" not in entry or "prefix" not in entry:
            raise MalformedArtifact(f"{path}[{i}]: each entry needs 'influx_db' and 'prefix'")
        db = entry["influx_db"]
        prefix = entry["prefix"]
        if not isinstance(db, str) or not isinstance(prefix, str):
            raise MalformedArtifact(f"{path}[{i}]: 'influx_db' and 'prefix' must be strings")
        if "prefixed" in entry:
            prefixed = entry["prefixed"]
            if not isinstance(prefixed, bool):
                raise MalformedArtifact(f"{path}[{i}]: 'prefixed' must be a boolean")
            if prefixed != bool(prefix):
                raise MalformedArtifact(
                    f"{path}[{i}]: 'prefixed'={prefixed} is inconsistent with prefix {prefix!r}"
                )
        if db in mapping and mapping[db] != prefix:
            raise MalformedArtifact(
                f"{path}: db {db!r} appears with conflicting prefixes "
                f"{mapping[db]!r} and {prefix!r}"
            )
        mapping[db] = prefix
    return mapping


def load_principals(path: str) -> List[Principal]:
    """Read principals.json into :class:`Principal` objects.

    Raises :class:`MalformedArtifact` on missing names, bad access values, or
    structurally invalid grants.
    """
    data = _load_json(path)
    if not isinstance(data, list):
        raise MalformedArtifact(f"{path}: expected a JSON array of principals")
    principals: List[Principal] = []
    for i, entry in enumerate(data):
        if not isinstance(entry, dict):
            raise MalformedArtifact(f"{path}[{i}]: expected an object")
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            raise MalformedArtifact(f"{path}[{i}]: 'name' must be a non-empty string")
        is_admin = bool(entry.get("is_admin", False))
        grants_raw = entry.get("grants") or []
        if not isinstance(grants_raw, list):
            raise MalformedArtifact(f"{path}[{i}]: 'grants' must be an array")
        grants: List[Grant] = []
        for j, g in enumerate(grants_raw):
            if not isinstance(g, dict) or "scope" not in g or "access" not in g:
                raise MalformedArtifact(
                    f"{path}[{i}].grants[{j}]: each grant needs 'scope' and 'access'"
                )
            scope = g["scope"]
            if not isinstance(scope, str) or not scope:
                raise MalformedArtifact(
                    f"{path}[{i}].grants[{j}]: 'scope' must be a non-empty string"
                )
            access = _parse_access(g["access"], f"{path}[{i}].grants[{j}]")
            grants.append(Grant(scope, access))
        principals.append(Principal(name, is_admin, grants))
    return principals


def build_entries(
    principals: List[Principal],
    scope_to_prefix: Dict[str, str],
    multi_scope_policy: str,
    password_map: Optional[Dict[str, str]] = None,
) -> List[AclEntry]:
    """Resolve principals into acl.conf entries.

    ``scope_to_prefix`` maps each migrated InfluxDB db/bucket to the table prefix
    it was written under (from the manifest, verbatim). A grant whose scope is
    absent from that map cannot be authorized -- no table was written for it -- so
    it is dropped with a warning; a non-admin left with no usable grant becomes a
    locked-down entry. ``multi_scope_policy`` is one of ``widest`` | ``skip`` |
    ``split-note``. Raises :class:`AclNameCollision` if two distinct usernames
    reduce to the same acl.conf key.
    """
    password_map = password_map or {}
    _check_name_collisions(principals)
    entries: List[AclEntry] = []
    for principal in principals:
        safe = _safe_name(principal.name)
        if safe is None:
            log.warning("skipping user %r: name has no safe acl.conf form", principal.name)
            continue
        password = password_map.get(principal.name) or _gen_password()

        if principal.is_admin:
            entries.append(AclEntry(safe, password, Access.RW, prefix=""))
            continue

        usable = _usable_grants(principal, scope_to_prefix)
        if not usable:
            # Either the principal had no grants at all, or every grant pointed
            # at a scope that was not migrated. Emit a locked-down read-only
            # entry on an unreachable prefix rather than silently granting all.
            log.warning(
                "user %s has no authorizable grants; emitting read-only, no-access entry",
                principal.name,
            )
            entries.append(AclEntry(safe, password, Access.RO, prefix=_NO_ACCESS_PREFIX))
            continue

        if len(usable) == 1:
            grant, prefix = usable[0]
            entries.append(AclEntry(safe, password, grant.access, prefix=prefix))
            continue

        entry = _resolve_multi(safe, password, principal, usable, multi_scope_policy)
        if entry is not None:
            entries.append(entry)
    return entries


def render(entries: List[AclEntry]) -> str:
    """Render entries to acl.conf text."""
    lines = [
        "# Generated by acl_from_artifacts.py from InfluxDB users/permissions.",
        "# Passwords are NOT migrated from InfluxDB (not recoverable); these are",
        "# operator-supplied or freshly generated. Keep this file readable only",
        "# by the server account.",
        "",
    ]
    for e in entries:
        lines.append(f"user.{e.name}.password={e.password}")
        lines.append(f"user.{e.name}.access={e.access.value}")
        if e.prefix:
            lines.append(f"user.{e.name}.prefix={e.prefix}")
        lines.append("")
    return "\n".join(lines)


def render_credentials_csv(entries: List[AclEntry]) -> str:
    """Render a username,password CSV for the operator to redistribute."""
    rows = ["username,password"]
    for e in entries:
        rows.append(f"{e.name},{e.password}")
    return "\n".join(rows) + "\n"


def load_password_map(path: Optional[str]) -> Dict[str, str]:
    """Load a ``name,password`` CSV used to reuse already-known passwords."""
    if not path:
        return {}
    mapping: Dict[str, str] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "," not in line:
                log.warning("ignoring malformed password-map line: %r", line)
                continue
            name, password = line.split(",", 1)
            mapping[name.strip()] = password.strip()
    return mapping


def _usable_grants(
    principal: Principal, scope_to_prefix: Dict[str, str]
) -> List[Tuple[Grant, str]]:
    """Return ``(grant, prefix)`` pairs for grants whose scope was migrated.

    A grant on a scope absent from the manifest (its measurements were filtered
    out or empty, so no table exists) has no prefix to bind to and is dropped.
    """
    usable: List[Tuple[Grant, str]] = []
    for g in principal.grants:
        if g.scope in scope_to_prefix:
            usable.append((g, scope_to_prefix[g.scope]))
        else:
            log.warning(
                "user %s has a grant on scope %r absent from the migration manifest "
                "(no tables written for it); dropping that grant",
                principal.name,
                g.scope,
            )
    return usable


def _resolve_multi(
    safe: str,
    password: str,
    principal: Principal,
    usable: List[Tuple[Grant, str]],
    policy: str,
) -> Optional[AclEntry]:
    scopes = ", ".join(g.scope for g, _ in usable)
    if policy == "skip":
        log.warning(
            "user %s spans multiple scopes (%s) and acl.conf is one-prefix-per-user; "
            "SKIPPING per --multi-scope-policy=skip",
            principal.name,
            scopes,
        )
        return None
    if policy == "split-note":
        first_grant, first_prefix = usable[0]
        dropped = ", ".join(g.scope for g, _ in usable[1:])
        log.warning(
            "user %s spans multiple scopes (%s); keeping first scope %s, DROPPING %s "
            "-- handle manually per --multi-scope-policy=split-note",
            principal.name,
            scopes,
            first_grant.scope,
            dropped,
        )
        return AclEntry(safe, password, first_grant.access, prefix=first_prefix)
    # Default: widest. Drop the prefix (all tables), access = least privilege of
    # the set (rw only if every grant is write-capable).
    access = Access.RW if all(g.access == Access.RW for g, _ in usable) else Access.RO
    log.warning(
        "user %s spans multiple scopes (%s); granting ALL tables with access=%s "
        "per --multi-scope-policy=widest (acl.conf cannot express multiple prefixes)",
        principal.name,
        scopes,
        access.value,
    )
    return AclEntry(safe, password, access, prefix="")


def _check_name_collisions(principals: List[Principal]) -> None:
    by_safe: Dict[str, List[str]] = {}
    for principal in principals:
        safe = _safe_name(principal.name)
        if safe is None:
            continue
        by_safe.setdefault(safe, [])
        if principal.name not in by_safe[safe]:
            by_safe[safe].append(principal.name)
    collisions = {safe: names for safe, names in by_safe.items() if len(names) > 1}
    if collisions:
        detail = "; ".join(f"{names} -> user.{safe}.*" for safe, names in collisions.items())
        raise AclNameCollision(
            "distinct InfluxDB usernames collide on the same acl.conf key; "
            "rename them upstream: " + detail
        )


def _parse_access(value: object, where: str) -> Access:
    try:
        return Access(value)
    except ValueError:
        raise MalformedArtifact(f"{where}: 'access' must be 'ro' or 'rw', got {value!r}")


def _load_json(path: str) -> object:
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        raise MalformedArtifact(f"{path}: file not found")
    except json.JSONDecodeError as e:
        raise MalformedArtifact(f"{path}: invalid JSON ({e})")


def _safe_name(name: str) -> Optional[str]:
    if not name:
        return None
    safe = "".join(c if c in _SAFE_NAME_CHARS else "_" for c in name)
    return safe or None


def _gen_password(length: int = 20) -> str:
    return "".join(secrets.choice(_PW_ALPHABET) for _ in range(length))


def _write(path: str, text: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    try:
        scope_to_prefix = load_manifest(args.manifest)
        principals = load_principals(args.principals)
    except MalformedArtifact as e:
        log.error("%s", e)
        return 2

    if not principals:
        log.warning("no principals in %s; nothing to write", args.principals)
        return 0

    password_map = load_password_map(args.password_map)
    try:
        entries = build_entries(principals, scope_to_prefix, args.multi_scope_policy, password_map)
    except AclNameCollision as e:
        log.error("%s", e)
        return 2

    if not entries:
        log.warning("no acl.conf entries produced (all skipped)")
        return 0

    acl_text = render(entries)
    if args.dry_run:
        log.info("[dry-run] acl.conf that would be written to %s:\n%s", args.acl_out, acl_text)
    else:
        _write(args.acl_out, acl_text)
        log.info("wrote %d ACL entries to %s", len(entries), args.acl_out)

    if args.credentials_out:
        if args.dry_run:
            log.info(
                "[dry-run] credentials CSV that would be written to %s (%d users)",
                args.credentials_out,
                len(entries),
            )
        else:
            _write(args.credentials_out, render_credentials_csv(entries))
            log.info("wrote credentials for %d users to %s", len(entries), args.credentials_out)

    return 0


def _parse_args(argv: Optional[List[str]]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="acl_from_artifacts.py",
        description="Generate the fork's conf/acl.conf from InfluxDB migration artifacts.",
    )
    p.add_argument("--manifest", default="migration-manifest.json", help="path to migration-manifest.json")
    p.add_argument("--principals", default="principals.json", help="path to principals.json")
    p.add_argument("--acl-out", default="conf/acl.conf", help="write generated acl.conf here")
    p.add_argument("--credentials-out", help="write generated username,password CSV here")
    p.add_argument("--password-map", help="CSV name,password to reuse known passwords")
    p.add_argument(
        "--multi-scope-policy",
        choices=["widest", "skip", "split-note"],
        default="widest",
        help="how to map a user with grants on multiple databases",
    )
    p.add_argument("--dry-run", action="store_true", help="report without writing")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(main())
