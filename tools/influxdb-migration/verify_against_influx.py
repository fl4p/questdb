#!/usr/bin/env python3
"""Spot-check migrated QuestDB values against the InfluxDB source of truth.

The migration downsampled to a 20s grid (last value per field per
``(measurement+tags, 20s-bucket)``), so a faithful check re-derives that same
"last value per bucket" from the RAW source and compares. It reads the source the
exact way the import did -- ``influxd inspect export-lp`` over a narrow window (no
Flux, so no cardinality blow-up) -- recomputes bucket lasts for a chosen series,
and diffs them against QuestDB.

Per random window it runs ONE export, then compares several random series (full
tagset device/did/uid/addrh/slug). Windows are aligned to 20s and only buckets
FULLY inside the window are compared (a partial bucket would miss points and
falsely mismatch). FLOAT compares within a float32-relative tolerance (QuestDB
stores FLOAT as 4 bytes); INT compares after truncation (import coerced
float->int); nulls must agree.
"""

from __future__ import annotations

import json
import random
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

QDB = "http://localhost:9000"
ENGINE = "/mnt/HC_Vol32/influxdb/engine"
BUCKET = "21bb6302e4d8bc07"
INTERVAL_NS = 20_000_000_000
INTERVAL_US = 20_000_000
TAGS = ("device", "did", "uid", "addrh", "slug")
FIELDS = [
    ("voltage", "float"),
    ("current", "float"),
    ("soc", "float"),
    ("voltage_cell000", "int"),
    ("voltage_cell001", "int"),
    ("voltage_cell010", "int"),
]
FIELD_SET = {f for f, _ in FIELDS}


def qexec(sql: str):
    url = QDB + "/exec?" + urllib.parse.urlencode({"query": sql})
    with urllib.request.urlopen(url, timeout=120) as r:
        return json.load(r)


def pick_series(day: str, limit: int):
    cols = ", ".join(TAGS)
    res = qexec(
        f"SELECT {cols}, count() n FROM batmon_tele_batmon "
        f"WHERE timestamp IN '{day}' GROUP BY {cols}"
    )
    rows = [r for r in res["dataset"] if r[len(TAGS)] > 50]
    random.shuffle(rows)
    return [{TAGS[i]: row[i] for i in range(len(TAGS))} for row in rows[:limit]]


def us_to_iso(us: int) -> str:
    # windows are 20s-aligned (whole seconds), so second precision is exact
    return datetime.fromtimestamp(us // 1_000_000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def export_window(start_iso: str, end_iso: str) -> list:
    p = subprocess.run(
        ["sudo", "-n", "/usr/bin/influxd", "inspect", "export-lp",
         "--engine-path", ENGINE, "--bucket-id", BUCKET, "--measurement", "batmon",
         "--start", start_iso, "--end", end_iso, "--output-path", "-"],
        capture_output=True, text=True,
    )
    if p.returncode != 0:
        sys.stderr.write(p.stderr[-500:])
    return p.stdout.splitlines()


def parse_head_tags(head: str) -> dict:
    tags = {}
    for kv in head.split(",")[1:]:
        eq = kv.find("=")
        if eq > 0:
            tags[kv[:eq]] = kv[eq + 1:]
    return tags


def influx_bucket_lasts(lines, series, ws_ns, we_ns):
    """(field, label_us) -> last value, for buckets fully inside [ws_ns, we_ns)."""
    best = {f: {} for f in FIELD_SET}
    for line in lines:
        sp = line.find(" ")
        if sp < 0:
            continue
        head = line[:sp]
        rest = line[sp + 1:]
        lsp = rest.rfind(" ")
        if lsp < 0:
            continue
        field_tok = rest[:lsp]
        eq = field_tok.find("=")
        if eq < 0:
            continue
        fname = field_tok[:eq]
        if fname not in FIELD_SET:
            continue
        if parse_head_tags(head) != series:
            continue
        try:
            ts = int(rest[lsp + 1:])
        except ValueError:
            continue
        bucket = (ts // INTERVAL_NS) * INTERVAL_NS
        if bucket < ws_ns or bucket + INTERVAL_NS > we_ns:
            continue  # only fully-covered buckets
        label_us = (bucket + INTERVAL_NS) // 1000
        slot = best[fname].get(label_us)
        if slot is None or ts >= slot[0]:
            best[fname][label_us] = (ts, field_tok[eq + 1:])
    return best


def coerce(kind, raw):
    raw = raw.rstrip("iI")
    if raw == "":
        return None
    v = float(raw)
    return int(v) if kind == "int" else v


def values_equal(kind, qv, iv) -> bool:
    if qv is None or iv is None:
        return qv is None and iv is None
    if kind == "int":
        return int(qv) == int(iv)
    a, b = float(qv), float(iv)
    return a == b or abs(a - b) <= 1e-6 * max(abs(a), abs(b), 1.0)


def compare(series, ws_ns, we_ns, lines):
    inf = influx_bucket_lasts(lines, series, ws_ns, we_ns)
    cols = ", ".join(f for f, _ in FIELDS)
    where = " AND ".join(f"{k}='{v}'" for k, v in series.items())
    # QuestDB row label = bucket+20s; full buckets in [ws,we) -> labels in (ws, we].
    lo_us, hi_us = ws_ns // 1000, we_ns // 1000
    res = qexec(
        f"SELECT cast(timestamp as long) ts, {cols} FROM batmon_tele_batmon WHERE {where} "
        f"AND timestamp > {lo_us} AND timestamp <= {hi_us} ORDER BY timestamp"
    )
    checks = mism = 0
    q_labels = set()
    for row in res["dataset"]:
        lbl = row[0]
        q_labels.add(lbl)
        for i, (fname, kind) in enumerate(FIELDS):
            qv = row[i + 1]
            slot = inf[fname].get(lbl)
            iv = coerce(kind, slot[1]) if slot else None
            checks += 1
            if not values_equal(kind, qv, iv):
                mism += 1
                if mism <= 6:
                    print(f"    MISMATCH {fname} @us{lbl}: qdb={qv!r} influx={iv!r}")
    # rows present in influx but missing in QuestDB (any field)
    inf_labels = {lbl for f in FIELD_SET for lbl in inf[f]}
    missing = inf_labels - q_labels
    if missing:
        print(f"    {len(missing)} bucket(s) in influx but not QuestDB (e.g. us{sorted(missing)[:3]})")
    return checks, mism, len(res["dataset"]), len(missing)


def main(argv=None):
    args = argv if argv is not None else sys.argv[1:]
    n_windows = int(args[0]) if len(args) > 0 else 3
    series_per = int(args[1]) if len(args) > 1 else 3
    win_s = int(args[2]) if len(args) > 2 else 180
    day = "2026-05-31"
    day_start_us = qexec(
        f"SELECT cast(cast('{day}T00:00:00.000000Z' as timestamp) as long) t"
    )["dataset"][0][0]
    day_len_us = 86_400_000_000
    tot_c = tot_m = tot_missing = 0
    for w in range(n_windows):
        # align window start to a 20s boundary (us), span = win_s seconds
        off = random.randint(0, day_len_us - win_s * 1_000_000 - INTERVAL_US)
        ws_us = ((day_start_us + off) // INTERVAL_US) * INTERVAL_US
        we_us = ws_us + win_s * 1_000_000
        ws_ns, we_ns = ws_us * 1000, we_us * 1000
        s_iso, e_iso = us_to_iso(ws_us), us_to_iso(we_us)
        print(f"[window {w+1}/{n_windows}] {s_iso} .. {e_iso}")
        lines = export_window(s_iso, e_iso)
        for s in pick_series(day, series_per):
            c, m, qrows, miss = compare(s, ws_ns, we_ns, lines)
            status = "OK" if (m == 0 and miss == 0) else f"{m} mism, {miss} missing"
            print(f"  did={s['did']} device={s['device']}: {qrows} rows, {c} checks -> {status}")
            tot_c += c
            tot_m += m
            tot_missing += miss
    print(f"\nTOTAL: {tot_c} value-checks, {tot_m} mismatches, {tot_missing} missing rows")
    return 1 if (tot_m or tot_missing) else 0


if __name__ == "__main__":
    raise SystemExit(main())
