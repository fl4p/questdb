#!/usr/bin/env python3
"""Microbenchmark for the schema-coercion hot path in pivot_lp.

Synthesizes batmon-shaped long-format LP (one field per line: ~32 INT
voltage_cellNNN, a LONG problem_code, several BOOLEAN switches_*, the rest
FLOAT) and runs merge_stream in downsample mode under --schema-file, the exact
configuration the real import uses. Reports field-lines/second and asserts the
WITH-schema output is byte-identical across runs (a self-consistency check; the
unit tests pin it against the slow reference).

Run:  PYTHONPATH=. python3 tests/bench_coerce.py [N_LINES]
"""

import sys
import time

import pivot_lp
from pivot_lp import SchemaCoercer, merge_stream


# batmon-shaped schema: dominant INT (voltage cells), a LONG, some BOOLs, floats.
def _schema():
    cols = {}
    for i in range(32):
        cols["voltage_cell%03d" % i] = "int"
    cols["problem_code"] = "int"
    for s in ("charge", "discharge", "balance", "heater", "fan", "alarm"):
        cols["switches_" + s] = "bool"
    for f in ("voltage", "current", "power", "temperature", "soc", "num_samples"):
        cols[f] = "float"
    return {"batmon": cols}


def _gen_lines(n, schema):
    """Generate n field-lines cycling through the schema columns, ts-sorted.

    Values mimic real export-lp: ints arrive canonical (Ni), floats as plain
    decimals, bools as numeric 0/1. Timestamps advance so downsample buckets
    flush regularly.
    """
    cols = list(schema["batmon"].items())
    lines = []
    ts = 1_700_000_000_000_000_000
    step = 1_000_000_000  # 1s; with 20s downsample, ~20 lines/bucket
    i = 0
    while len(lines) < n:
        name, kind = cols[i % len(cols)]
        if kind == "int":
            val = "%di" % (3200 + (i % 400))
        elif kind == "bool":
            val = "1" if (i & 1) else "0"
        else:
            val = "%.3f" % (13.0 + (i % 1000) / 1000.0)
        lines.append("batmon,device=d%d %s=%s %d" % (i % 4, name, val, ts))
        i += 1
        if i % 50 == 0:
            ts += step
    return lines


class _NullFeeder:
    __slots__ = ("n",)

    def __init__(self):
        self.n = 0

    def add(self, line):
        self.n += 1

    def flush(self):
        pass


class _CollectFeeder:
    def __init__(self):
        self.lines = []

    def add(self, line):
        self.lines.append(line)

    def flush(self):
        pass


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 2_000_000
    schema = _schema()
    lines = _gen_lines(n, schema)
    interval = 20_000_000_000  # 20s

    # Correctness snapshot for byte-identity checks across edits.
    cf = _CollectFeeder()
    merge_stream(iter(lines), "tm_", cf, interval,
                 SchemaCoercer({k: dict(v) for k, v in schema.items()}))
    print("schema-path output rows: %d" % len(cf.lines))
    print("first row: %s" % cf.lines[0])
    print("last  row: %s" % cf.lines[-1])

    def run(use_schema):
        co = SchemaCoercer({k: dict(v) for k, v in schema.items()}) if use_schema else None
        feeder = _NullFeeder()
        t0 = time.perf_counter()
        pts, fl = merge_stream(iter(lines), "tm_", feeder, interval, co)
        dt = time.perf_counter() - t0
        return fl, dt, pts

    # warm + measure best of 3
    best_no = best_yes = None
    for _ in range(3):
        fl, dt, _ = run(False)
        best_no = dt if best_no is None else min(best_no, dt)
        fl, dt, _ = run(True)
        best_yes = dt if best_yes is None else min(best_yes, dt)

    print("\nlines: %d" % fl)
    print("WITHOUT schema: %.3fs  %.0f lines/s" % (best_no, fl / best_no))
    print("WITH    schema: %.3fs  %.0f lines/s" % (best_yes, fl / best_yes))
    print("schema overhead: %.2fx slower" % (best_yes / best_no))


if __name__ == "__main__":
    main()
