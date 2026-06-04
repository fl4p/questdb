#!/usr/bin/env python3
"""Benchmark the pivot hot path: CURRENT pivot_lp.py vs the version right after
the QuestDB WAL back-pressure (_WalThrottle/_ThrottledFeeder) landed.

The back-pressure commit is f78a184e77 ("Add export-lp bulk import with wide
pivot and opt-in indexing"). Everything since then (schema enforcement, the
coercer speedup, folding the per-line progress wrapper into the loop) is on top.

This drives each version's merge_stream EXACTLY the way its own main() does:

  OLD f78a184e77 : merge_stream(ticking(iter(lines)), prefix, feeder, interval)
                   -- a per-line generator wrapper around the input (the ~38%
                   ticking overhead noted in the profile).
  CURRENT        : merge_stream(iter(lines), prefix, feeder, interval, None,
                   on_progress) -- progress folded into the loop, no wrapper.

Both run in DOWNSAMPLE mode (20s, the production config) with a counting feeder
(no network) and NO schema coercer (the old version has none -- apples to
apples). Reports field-lines/second; higher is better.

Run:  PYTHONPATH=. python3 tests/bench_versions.py [N_LINES]
NOTE: production runs under PyPy; CPython magnitudes differ but the relative
ordering of these structural changes holds. Run under pypy3 if available.
"""

import importlib.util
import os
import subprocess
import sys
import time

OLD_REF = "f78a184e77"

HERE = os.path.dirname(os.path.abspath(__file__))
TOOLDIR = os.path.dirname(HERE)


def _schema_cols():
    # batmon-shaped: dominant INT (voltage cells), a LONG, some BOOLs, floats.
    cols = []
    for i in range(32):
        cols.append(("voltage_cell%03d" % i, "int"))
    cols.append(("problem_code", "int"))
    for s in ("charge", "discharge", "balance", "heater", "fan", "alarm"):
        cols.append(("switches_" + s, "bool"))
    for f in ("voltage", "current", "power", "temperature", "soc", "num_samples"):
        cols.append((f, "float"))
    return cols


def gen_lines(n):
    """Generate n single-field LP lines, timestamp-sorted (downsample needs it)."""
    cols = _schema_cols()
    out = []
    ts = 1_700_000_000_000_000_000
    step = 1_000_000_000  # 1s; with 20s downsample, ~20 lines/bucket
    i = 0
    nc = len(cols)
    while len(out) < n:
        name, kind = cols[i % nc]
        if kind == "int":
            val = "%di" % (3200 + (i % 400))
        elif kind == "bool":
            val = "1" if (i & 1) else "0"
        else:
            val = "%.3f" % (13.0 + (i % 1000) / 1000.0)
        out.append("batmon,device=d%d %s=%s %d" % (i % 4, name, val, ts))
        i += 1
        if i % 50 == 0:
            ts += step
    return out


def load_old_module():
    """git show the old pivot_lp.py into a temp file and import it."""
    src = subprocess.check_output(
        ["git", "-C", TOOLDIR, "show", "%s:tools/influxdb-migration/pivot_lp.py" % OLD_REF],
        text=True,
    )
    tmp = os.path.join(HERE, "_pivot_lp_old_gen.py")
    with open(tmp, "w") as fh:
        fh.write(src)
    spec = importlib.util.spec_from_file_location("pivot_lp_old", tmp)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["pivot_lp_old"] = mod
    spec.loader.exec_module(mod)
    return mod, tmp


class NullFeeder:
    __slots__ = ("n",)

    def __init__(self):
        self.n = 0

    def add(self, line):
        self.n += 1

    def flush(self):
        pass


def make_ticking():
    """Replica of OLD main()'s ticking() input wrapper (the per-line generator)."""
    t0 = time.monotonic()
    state = {"n": 0, "last": 0.0}

    def ticking(src):
        for line in src:
            state["n"] += 1
            if state["n"] % 200_000 == 0:
                now = time.monotonic()
                if now - state["last"] >= 2.0:
                    state["last"] = now
            yield line

    return ticking


def make_on_progress():
    """Replica of CURRENT _make_on_progress (self-limited, no per-line wrapper)."""
    t0 = time.monotonic()
    last = [0.0]

    def on_progress(n):
        now = time.monotonic()
        if now - last[0] >= 2.0:
            last[0] = now

    return on_progress


def run_old(mod, lines, interval):
    ticking = make_ticking()
    feeder = NullFeeder()
    t0 = time.perf_counter()
    pts, fl = mod.merge_stream(ticking(iter(lines)), "tm_", feeder, interval)
    return time.perf_counter() - t0, pts, fl


def run_current(mod, lines, interval):
    on_progress = make_on_progress()
    feeder = NullFeeder()
    t0 = time.perf_counter()
    pts, fl = mod.merge_stream(iter(lines), "tm_", feeder, interval, None, on_progress)
    return time.perf_counter() - t0, pts, fl


def best_of(fn, mod, lines, interval, reps=4):
    best = None
    pts = fl = 0
    for _ in range(reps):
        dt, pts, fl = fn(mod, lines, interval)
        best = dt if best is None else min(best, dt)
    return best, pts, fl


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 3_000_000
    interval = 20_000_000_000  # 20s downsample (production config)

    print("python: %s" % sys.version.split()[0])
    print("generating %s field-lines..." % "{:,}".format(n))
    lines = gen_lines(n)

    import pivot_lp as cur  # noqa: E402

    old, tmp = load_old_module()
    try:
        print("OLD ref:  %s  (right after WAL back-pressure)" % OLD_REF)
        print("CURRENT:  %s\n" % subprocess.check_output(
            ["git", "-C", TOOLDIR, "rev-parse", "--short", "HEAD"], text=True).strip())

        o_dt, o_pts, o_fl = best_of(run_old, old, lines, interval)
        c_dt, c_pts, c_fl = best_of(run_current, cur, lines, interval)

        assert o_pts == c_pts, "point count mismatch: old=%d cur=%d" % (o_pts, c_pts)
        assert o_fl == c_fl, "field-line count mismatch: old=%d cur=%d" % (o_fl, c_fl)

        print("field-lines: %s   merged points: %s\n" % (
            "{:,}".format(c_fl), "{:,}".format(c_pts)))
        print("OLD (%s):   %.3fs   %s lines/s" % (
            OLD_REF, o_dt, "{:,}".format(int(o_fl / o_dt))))
        print("CURRENT:        %.3fs   %s lines/s" % (
            c_dt, "{:,}".format(int(c_fl / c_dt))))
        speedup = o_dt / c_dt
        if speedup >= 1.0:
            print("\n=> CURRENT is %.2fx FASTER than the back-pressure version" % speedup)
        else:
            print("\n=> CURRENT is %.2fx SLOWER (regression!) than back-pressure" % (1 / speedup))
    finally:
        os.remove(tmp)
        pc = tmp + "c"
        if os.path.exists(pc):
            os.remove(pc)


if __name__ == "__main__":
    main()
