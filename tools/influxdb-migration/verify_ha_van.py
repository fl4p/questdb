#!/usr/bin/env python3
"""Spot-check migrated ha_van values against the InfluxDB source.

The ha_van "others" tables are loaded RAW (value-only, no downsample), so each
QuestDB row is just one source point's ``value`` at its exact timestamp -- no
bucket-last logic needed. For random (measurement, entity, window) samples this
re-reads the source via ``export-lp`` (filtered to the measurement + entity),
keeps the ``value`` field, and compares value-by-value to QuestDB.

QuestDB stores the timestamp as microseconds (ILP ns truncated), so points are
keyed by us-epoch. Numeric values compare within a tiny relative tolerance
(text round-trip through ILP); string values (e.g. ``state``) compare exactly.
"""

from __future__ import annotations

import json
import random
import subprocess
import sys
import urllib.parse
import urllib.request

from sanitize_lp import MAP

QDB = "http://localhost:9000"
ENGINE = "/mnt/HC_Vol32/influxdb/engine"
BUCKETS = {"ha_van": "2480772d801b9374", "ha_van_dn": "32d1e9dca26087c9"}
# Original measurement names to sample (numeric; skip mppt=downsampled, state=string).
SAMPLE_MEAS = ["V", "A", "W", "Wh", "kWh", "%", "°C", "hPa", "lx", "Ah"]


def qexec(sql: str):
    url = QDB + "/exec?" + urllib.parse.urlencode({"query": sql})
    with urllib.request.urlopen(url, timeout=120) as r:
        return json.load(r)


def export_lines(bid, meas, s_iso, e_iso):
    p = subprocess.run(
        ["sudo", "-n", "/usr/bin/influxd", "inspect", "export-lp",
         "--engine-path", ENGINE, "--bucket-id", bid, "--measurement", meas,
         "--start", s_iso, "--end", e_iso, "--output-path", "-"],
        capture_output=True, text=True,
    )
    if p.returncode != 0:
        sys.stderr.write(p.stderr[-300:])
    return p.stdout.splitlines()


def first_unescaped_space(s):
    i, n = 0, len(s)
    while i < n:
        if s[i] == "\\":
            i += 2
            continue
        if s[i] == " ":
            return i
        i += 1
    return -1


def influx_values(lines, entity):
    """us-epoch -> value(str) for the given entity_id, value field only."""
    needle = f"entity_id={entity}"
    out = {}
    for line in lines:
        sp1 = first_unescaped_space(line)
        sp2 = line.rfind(" ")
        if sp1 < 0 or sp2 <= sp1:
            continue
        head, field, ts = line[:sp1], line[sp1 + 1:sp2], line[sp2 + 1:]
        if field[:6] != "value=":
            continue
        # entity match (tag substring with delimiters)
        if needle not in head:
            continue
        # exact tag check (avoid prefix collisions)
        if not any(t == needle for t in head.split(",")[1:]):
            continue
        try:
            us = int(ts) // 1000
        except ValueError:
            continue
        out[us] = field[6:]
    return out


def num(v):
    try:
        return float(v.strip('"').rstrip("iI"))
    except ValueError:
        return None


def equalish(qv, iv):
    if qv is None or iv is None:
        return qv is None and iv is None
    a, b = num(str(qv)), num(iv)
    if a is not None and b is not None:
        return a == b or abs(a - b) <= 1e-6 * max(abs(a), abs(b), 1.0)
    return str(qv) == iv.strip('"')  # string compare (e.g. state)


def sql_str(s: str) -> str:
    """Escape a value for a single-quoted QuestDB string literal."""
    return s.replace("'", "''")


def diff_rows(qrows, inf, ws, we):
    """Compare QuestDB rows against the influx source for one [ws, we) window.

    ``qrows``: list of (us_ts, value) read from QuestDB (already restricted to
    the window). ``inf``: {us_ts: value_str} parsed from the influx export.

    Returns (n_checked, mismatches, missing):
      * ``mismatches`` = [(ts, qv, iv), ...] -- QDB rows whose value disagrees
        with influx (or that influx lacks);
      * ``missing`` = [(ts, iv), ...] -- influx points inside the window that
        QuestDB does NOT have. This is the silent-data-loss signal: checking
        only QDB->influx (the old behavior) passes even when whole rows were
        dropped, so the reverse direction is the one that actually proves the
        migration was lossless.

    export-lp's ``--end`` can be inclusive, so a point exactly at ``we`` may be
    in ``inf``; the half-open ``k < we`` filter excludes it (QuestDB's window is
    ``timestamp < we``), avoiding a false "missing".
    """
    qkeys = set()
    mismatches = []
    for ts, qv in qrows:
        qkeys.add(ts)
        if not equalish(qv, inf.get(ts)):
            mismatches.append((ts, qv, inf.get(ts)))
    missing = [(k, inf[k]) for k in sorted(inf) if ws <= k < we and k not in qkeys]
    return len(qrows), mismatches, missing


def main(argv=None):
    args = argv if argv is not None else sys.argv[1:]
    n_samples = int(args[0]) if args else 8
    win_s = int(args[1]) if len(args) > 1 else 600
    checked = total_c = total_m = total_missing = 0
    for _ in range(n_samples):
        bname = random.choice(list(BUCKETS))
        bid = BUCKETS[bname]
        meas = random.choice(SAMPLE_MEAS)
        table = f"{bname}_{MAP[meas]}"
        # random entity + window from the table
        try:
            ent = qexec(
                f"SELECT entity_id, count() c FROM {table} GROUP BY entity_id ORDER BY c DESC LIMIT 20"
            )["dataset"]
        except Exception:
            continue
        ent = [e for e in ent if e[0]]
        if not ent:
            print(f"  {table}: no entities, skip")
            continue
        entity = random.choice(ent)[0]
        eq = sql_str(entity)
        rng = qexec(
            f"SELECT cast(min(timestamp) as long), cast(max(timestamp) as long) "
            f"FROM {table} WHERE entity_id='{eq}'"
        )["dataset"][0]
        lo, hi = rng[0], rng[1]
        if not lo or hi <= lo:
            continue
        # Floor the window start to a whole second so the second-precision ISO
        # export bound is exact; win_s is whole seconds, so we stays aligned.
        # Otherwise QDB could return rows in [floor(we,1s), we) that the influx
        # export (truncated to seconds) never emitted -- false mismatches.
        ws = random.randint(lo, max(lo, hi - win_s * 1_000_000))
        ws -= ws % 1_000_000
        we = ws + win_s * 1_000_000
        s_iso = us_to_iso(ws)
        e_iso = us_to_iso(we)
        inf = influx_values(export_lines(bid, meas, s_iso, e_iso), entity)
        qrows = qexec(
            f"SELECT cast(timestamp as long) ts, value FROM {table} "
            f"WHERE entity_id='{eq}' AND timestamp >= {ws} AND timestamp < {we} ORDER BY timestamp"
        )["dataset"]
        c, mismatches, missing = diff_rows(qrows, inf, ws, we)
        for ts, qv, iv in mismatches[:4]:
            print(f"    MISMATCH {table} {entity} @us{ts}: qdb={qv!r} influx={iv!r}")
        for ts, iv in missing[:4]:
            print(f"    MISSING  {table} {entity} @us{ts}: influx={iv!r} not in QuestDB")
        m = len(mismatches)
        miss = len(missing)
        parts = []
        if m:
            parts.append(f"{m} MISMATCH")
        if miss:
            parts.append(f"{miss} MISSING")
        status = "OK" if not parts else ", ".join(parts)
        print(f"  {table} entity={entity} {s_iso[:19]} +{win_s}s: {c} rows -> {status}")
        checked += 1
        total_c += c
        total_m += m
        total_missing += miss
    print(
        f"\nTOTAL: {checked} samples, {total_c} value-checks, "
        f"{total_m} mismatches, {total_missing} rows in influx but missing from QuestDB"
    )
    return 1 if (total_m or total_missing) else 0


def us_to_iso(us):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(us // 1_000_000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


if __name__ == "__main__":
    raise SystemExit(main())
