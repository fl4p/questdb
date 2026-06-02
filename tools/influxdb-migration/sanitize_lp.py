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


def main() -> int:
    out = sys.stdout
    write = out.write
    dropped = 0
    kept = 0
    for line in sys.stdin:
        if not line or line[0] == "#":
            continue
        end = _measurement_end(line)
        literal = _unescape_measurement(line[:end])
        name = sanitized(literal)
        if name is None:
            dropped += 1
            continue
        # ``name`` is a clean identifier, so no re-escaping is needed.
        write(name + line[end:])
        kept += 1
    out.flush()
    sys.stderr.write(f"sanitize_lp: kept {kept} lines, dropped {dropped}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
