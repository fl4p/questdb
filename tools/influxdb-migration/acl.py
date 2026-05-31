"""Generate the fork's file-based ACL (conf/acl.conf) from InfluxDB principals.

The fork's ACL (see qdb-inf/docs/ACL.md) is one block per user:

    user.<name>.password=<plaintext>
    user.<name>.access=ro|rw          # default rw
    user.<name>.prefix=<tablePrefix>  # optional; empty = all tables

InfluxDB never exposes existing passwords (v1 stores hashes, v2 uses tokens), so
this module either reuses an operator-supplied password map or generates strong
random passwords and emits a credentials CSV for redistribution.

The model has one impedance mismatch: an InfluxDB principal may hold grants on
several scopes, but acl.conf allows only ONE prefix + ONE access per username.
``--multi-scope-policy`` decides how to resolve that, always logging the choice.
"""

from __future__ import annotations

import logging
import secrets
import string
from dataclasses import dataclass
from typing import Dict, List, Optional

from model import Access, Principal, scope_prefix

log = logging.getLogger("influx_migrate.acl")

_PW_ALPHABET = string.ascii_letters + string.digits
# acl.conf keys are dotted (user.<name>.password); anything outside
# [A-Za-z0-9_] (dots, dashes, '=', whitespace) could break key parsing, so
# reduce names to that charset and hard-fail if two distinct names collide.
_SAFE_NAME = set(string.ascii_letters + string.digits + "_")


class AclNameCollision(ValueError):
    """Raised when two distinct InfluxDB usernames map to the same acl.conf key.

    acl.conf keys are ``user.<name>.*``; if 'alice.x' and 'alice-x' both reduce
    to the safe key 'alice_x', emitting both would produce duplicate keys with
    different passwords/access -- undefined loader behavior. We hard-fail and
    name the colliding users rather than silently clobbering one.
    """


@dataclass
class AclEntry:
    name: str
    password: str
    access: Access
    prefix: str  # "" means all tables (no restriction)


def build_entries(
    principals: List[Principal],
    multi_scope_policy: str,
    password_map: Optional[Dict[str, str]] = None,
    use_prefix: bool = True,
) -> List[AclEntry]:
    """Resolve principals into acl.conf entries under the chosen policy.

    ``multi_scope_policy`` is one of ``widest`` | ``skip`` | ``split-note``.
    With ``use_prefix`` False (single-DB ``--no-prefix`` migrations) every entry
    gets an empty prefix, since tables are bare and a ``<db>_`` prefix would
    match nothing and lock the user out. Raises :class:`AclNameCollision` if two
    distinct usernames reduce to the same acl.conf key.
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

        if principal.is_admin or not principal.grants:
            if not principal.grants and not principal.is_admin:
                # A non-admin with zero grants can touch nothing; emit a
                # locked-down read-only entry with an impossible prefix rather
                # than silently granting all tables. (Stays unreachable even
                # under --no-prefix, where no table name is "__noaccess__...".)
                log.warning(
                    "user %s has no grants; emitting read-only, no-access entry",
                    principal.name,
                )
                entries.append(AclEntry(safe, password, Access.RO, prefix="__noaccess__"))
                continue
            entries.append(AclEntry(safe, password, Access.RW, prefix=""))
            continue

        if len(principal.grants) == 1:
            grant = principal.grants[0]
            prefix = scope_prefix(grant.scope) if use_prefix else ""
            entries.append(AclEntry(safe, password, grant.access, prefix=prefix))
            continue

        # Multiple grants: resolve per policy.
        entry = _resolve_multi(safe, password, principal, multi_scope_policy, use_prefix)
        if entry is not None:
            entries.append(entry)
    return entries


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


def _resolve_multi(
    safe: str, password: str, principal: Principal, policy: str, use_prefix: bool
) -> Optional[AclEntry]:
    scopes = ", ".join(g.scope for g in principal.grants)
    if policy == "skip":
        log.warning(
            "user %s spans multiple scopes (%s) and acl.conf is one-prefix-per-user; "
            "SKIPPING per --multi-scope-policy=skip",
            principal.name,
            scopes,
        )
        return None
    if policy == "split-note":
        first = principal.grants[0]
        dropped = ", ".join(g.scope for g in principal.grants[1:])
        log.warning(
            "user %s spans multiple scopes (%s); keeping first scope %s, "
            "DROPPING %s -- handle manually per --multi-scope-policy=split-note",
            principal.name,
            scopes,
            first.scope,
            dropped,
        )
        prefix = scope_prefix(first.scope) if use_prefix else ""
        return AclEntry(safe, password, first.access, prefix=prefix)
    # Default: widest. Drop the prefix (all tables), access = least privilege of
    # the set (rw only if every grant is write-capable).
    access = Access.RW if all(g.access == Access.RW for g in principal.grants) else Access.RO
    log.warning(
        "user %s spans multiple scopes (%s); granting ALL tables with access=%s "
        "per --multi-scope-policy=widest (acl.conf cannot express multiple prefixes)",
        principal.name,
        scopes,
        access.value,
    )
    return AclEntry(safe, password, access, prefix="")


def render(entries: List[AclEntry]) -> str:
    """Render entries to acl.conf text."""
    lines = [
        "# Generated by influx_migrate.py from InfluxDB users/permissions.",
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
        # No InfluxDB password is ever placed in source data, so a CSV here only
        # contains values this run created or the operator already knew.
        rows.append(f"{e.name},{e.password}")
    return "\n".join(rows) + "\n"


def _gen_password(length: int = 20) -> str:
    return "".join(secrets.choice(_PW_ALPHABET) for _ in range(length))


def _safe_name(name: str) -> Optional[str]:
    if not name:
        return None
    safe = "".join(c if c in _SAFE_NAME else "_" for c in name)
    return safe or None
