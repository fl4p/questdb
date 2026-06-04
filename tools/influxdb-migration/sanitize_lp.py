#!/usr/bin/env python3
"""Rewrite line-protocol measurement names to valid QuestDB identifiers.

Home Assistant's InfluxDB schema uses the unit_of_measurement AS the measurement
name (``%``, ``kWh/d``, ``°C``, ``pending update(s)`` ...), almost none of which
satisfy QuestDB's ``[A-Za-z0-9_]`` table-naming rule. This filter rewrites the
measurement on each LP line to an approved, sanitized name (the downstream pivot
then adds the ``<bucket>_`` prefix), and drops measurements on the skip list.

A known measurement maps via ``MAP`` (the names approved for the ha_van import).
An UNKNOWN measurement is sanitized generically and logged once, so a new unit
never silently produces an illegal table name or gets dropped without notice.

Reads LP on stdin, writes rewritten LP on stdout. Measurement parsing/unescaping
reuses bulk_v1 so escaped commas/spaces in the original name are handled.
"""

from __future__ import annotations

import argparse
import re
import sys

from bulk_v1 import _measurement_end, _unescape_measurement

# Measurements dropped entirely (data not worth migrating).
#   ºC = U+00BA (masculine ordinal) -- an untagged orphan; the real Celsius data
#   lives under °C = U+00B0. Verified: ºC has 0 entity_id tags, ~8.5k points.
SKIP = {"ºC"}

# Approved explicit mapping for the ha_van / ha_van_dn buckets. Names already
# valid as identifiers are listed as identity so the table name is pinned and
# obvious rather than relying on the generic path.
MAP = {
    # renamed
    "%": "pct",
    "% available": "pct_available",
    "%/d": "pct_per_d",
    "F/m": "F_per_m",
    "KiB/s": "KiB_per_s",
    "kWh/d": "kWh_per_d",
    "kWh/h": "kWh_per_h",
    "km/h": "km_per_h",
    "packets/s": "packets_per_s",
    "pending update(s)": "pending_updates",
    "°C": "degC",  # ° = U+00B0
    # already-valid (identity, pinned)
    **{m: m for m in (
        "A", "Ah", "B", "GB", "GiB", "N", "V", "W", "Wh", "batmon", "cells",
        "dB", "dBm", "hPa", "kW", "kWh", "km", "lx", "m", "min", "mppt", "ms",
        "packets", "psi", "s", "smart_shunt", "state", "steps",
    )},
}

_warned: set = set()


def generic_sanitize(name: str) -> str:
    """Deterministic fallback for measurements not in MAP."""
    s = name.replace("°", "deg").replace("º", "deg")
    s = s.replace("%", "pct").replace("/", "_per_")
    s = re.sub(r"[^A-Za-z0-9_]+", "_", s).strip("_")
    s = re.sub(r"_+", "_", s)
    if not s:
        s = "unnamed"
    if s[0].isdigit():
        s = "m_" + s
    return s


def sanitized(literal: str):
    """Return the table-suffix for a measurement, or None to drop it."""
    if literal in SKIP:
        return None
    mapped = MAP.get(literal)
    if mapped is not None:
        return mapped
    g = generic_sanitize(literal)
    if literal not in _warned:
        _warned.add(literal)
        sys.stderr.write(
            f"sanitize_lp: unknown measurement {literal!r} -> {g!r} (generic)\n"
        )
    return g


def _first_unescaped_space(s: str) -> int:
    """Index of the first space not preceded by a backslash, or -1.

    The head (measurement + tag keys/values) escapes spaces as ``\\ ``; the
    head/fields boundary is the first UNescaped space. A plain find() would stop
    inside names like ``% available``.
    """
    i, n = 0, len(s)
    while i < n:
        c = s[i]
        if c == "\\":
            i += 2
            continue
        if c == " ":
            return i
        i += 1
    return -1


def split_line(line: str):
    """(head, field, ts) for a long-format LP line; None if malformed.

    head = "measurement,tags" (up to the first UNescaped space); field = the
    single "name=value" token; ts = trailing nanosecond timestamp. The last space
    delimits ts, so a quoted string value with internal spaces stays in ``field``.

    Strips all trailing whitespace, not just the newline: a stray trailing space
    would otherwise make ``rfind(" ")`` point past the real timestamp and emit a
    line with an empty ts (which QuestDB rejects).
    """
    line = line.rstrip()
    sp1 = _first_unescaped_space(line)
    sp2 = line.rfind(" ")
    if sp1 < 0 or sp2 <= sp1:
        return None
    return line[:sp1], line[sp1 + 1:sp2], line[sp2 + 1:]


def rebuild_head(head: str, new_meas: str, keep_tags) -> str:
    """Replace the measurement and drop any tag whose key is not in keep_tags.

    HA tags (entity_id, domain) carry no escaped commas, so a plain comma split
    is safe here; this is what strips dirty tag keys like 'Available (Important)'.
    """
    end = _measurement_end(head)
    tags = head[end + 1:] if end < len(head) and head[end] == "," else ""
    kept = [kv for kv in tags.split(",") if kv and kv.split("=", 1)[0] in keep_tags]
    return new_meas + ("," + ",".join(kept) if kept else "")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--value-only", action="store_true",
        help="keep only the 'value' field and the --keep-tags tags; drop all "
        "other fields/tags (HA attribute fields/tags have arbitrary names that "
        "are illegal QuestDB columns)",
    )
    p.add_argument("--value-field", default="value")
    p.add_argument("--keep-tags", default="entity_id,domain")
    args = p.parse_args(argv)
    keep_tags = set(t for t in args.keep_tags.split(",") if t)

    out = sys.stdout
    write = out.write
    dropped = kept = 0

    if not args.value_only:
        for line in sys.stdin:
            if not line or line[0] == "#":
                continue
            end = _measurement_end(line)
            name = sanitized(_unescape_measurement(line[:end]))
            if name is None:
                dropped += 1
                continue
            write(name + line[end:])  # name is clean, no re-escape needed
            kept += 1
    else:
        vfield = args.value_field
        for line in sys.stdin:
            if not line or line[0] == "#":
                continue
            parts = split_line(line)
            if parts is None:
                dropped += 1
                continue
            head, field, ts = parts
            if field.split("=", 1)[0] != vfield:
                dropped += 1  # not the value field -> drop (metadata/attribute)
                continue
            name = sanitized(_unescape_measurement(head[:_measurement_end(head)]))
            if name is None:
                dropped += 1
                continue
            write(rebuild_head(head, name, keep_tags) + " " + field + " " + ts + "\n")
            kept += 1

    out.flush()
    sys.stderr.write(f"sanitize_lp: kept {kept} lines, dropped {dropped}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
