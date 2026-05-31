"""Intermediate data model shared by readers, the writer, and ACL generation.

Both the InfluxDB v1 and v2 readers normalize their source-specific shapes into
these types, so the writer and ACL generator never need to know which InfluxDB
version the data came from.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List


class FieldType(str, Enum):
    """QuestDB-facing type of an InfluxDB field, decided from source schema.

    The string values double as human-readable labels in logs and dry-run
    output. The writer maps these onto the questdb Sender's typed column calls.
    """

    FLOAT = "float"
    INTEGER = "integer"
    STRING = "string"
    BOOLEAN = "boolean"


class Access(str, Enum):
    """Effective access level for an ACL principal grant."""

    RO = "ro"
    RW = "rw"


@dataclass
class TableSchema:
    """Column layout of one QuestDB table (one InfluxDB measurement).

    ``tag_keys`` become SYMBOL columns (InfluxDB tags); ``field_types`` map each
    field name to its QuestDB-facing type. The designated timestamp column is
    implicit (the row's ``ts_ns``) and is not listed here.
    """

    table: str
    tag_keys: List[str] = field(default_factory=list)
    field_types: Dict[str, FieldType] = field(default_factory=dict)


@dataclass
class Row:
    """A single point to write.

    ``tags`` and ``fields`` only contain keys that are actually present in the
    source point: InfluxDB series are sparse, so absent fields must NOT be
    written as NULL columns. ``ts_ns`` is an epoch timestamp in nanoseconds.
    """

    table: str
    tags: Dict[str, str]
    fields: Dict[str, object]
    ts_ns: int


@dataclass
class Grant:
    """One principal's access to one scope (db for v1, bucket for v2)."""

    scope: str
    access: Access


@dataclass
class Principal:
    """A user/token to replay into acl.conf.

    ``is_admin`` principals get full access with no prefix. Non-admin principals
    carry one or more :class:`Grant` entries; acl.conf can only express a single
    prefix + access per username, so multi-grant principals are resolved by the
    configured ``--multi-scope-policy`` in ``acl.py``.
    """

    name: str
    is_admin: bool = False
    grants: List[Grant] = field(default_factory=list)


@dataclass
class MigrationStats:
    """Per-table counters accumulated during a run (or a dry run)."""

    rows: int = 0
    fields_skipped_null: int = 0
    tags_skipped_empty: int = 0


class InvalidScopeName(ValueError):
    """Raised when a db/bucket name is not a clean QuestDB identifier.

    Per the agreed contract with the /query endpoint, the table prefix is the
    db name followed by a literal underscore with NO transformation. So a name
    that is not already ``[A-Za-z0-9_]+`` must be rejected and renamed upstream
    rather than silently sanitized (which would make ``?db=`` diverge from the
    actual table prefix).
    """


_CLEAN_NAME = re.compile(r"^[A-Za-z0-9_]+$")


def validate_scope(name: str) -> None:
    """Raise :class:`InvalidScopeName` if ``name`` is not a clean identifier."""
    if not name or not _CLEAN_NAME.match(name):
        raise InvalidScopeName(name)


def table_name(template: str, db: str, measurement: str, use_prefix: bool) -> str:
    """Build the QuestDB table name for a measurement.

    With ``use_prefix`` the name is ``template.format(db=db,
    measurement=measurement)`` (default template ``"{db}_{measurement}"``, a
    SINGLE underscore separator the read endpoint strips by exact match). The
    ``db`` must already be a clean identifier (see :func:`validate_scope`).
    Without a prefix the bare measurement is used (single-DB setups where the
    InfluxDB ``?db=`` parameter is empty).
    """
    if not use_prefix:
        return measurement
    return template.format(db=db, measurement=measurement)


def scope_prefix(db: str) -> str:
    """The acl.conf ``prefix`` value for a scope: ``<db>_`` (db already clean)."""
    return db + "_"
