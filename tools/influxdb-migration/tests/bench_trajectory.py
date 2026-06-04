#!/usr/bin/env python3
"""Benchmark raw merge_stream pivot speed across EVERY commit that touched
pivot_lp.py, to locate any peak/regression in the trajectory.

Unlike bench_versions.py (which faithfully replicates each version's main(),
including the old ticking() input wrapper), this measures the PURE merge_stream
algorithm: same input iterator for every version, no wrapper, no schema coercer
(coercer=None where the param exists). It introspects each version's signature
and only passes coercer/on_progress when supported. Downsample 20s, counting
feeder, best-of-4. Higher lines/s is better.

Run:  PYTHONPATH=. python3 tests/bench_trajectory.py [N_LINES]
"""

import importlib.util
import inspect
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
TOOLDIR = os.path.dirname(HERE)

from bench_versions import NullFeeder, gen_lines  # noqa: E402


def commits_touching():
    out = subprocess.check_output(
        ["git", "-C", TOOLDIR, "log", "--reverse", "--format=%h\t%s", "--",
         "pivot_lp.py"],
        text=True,
    )
    rows = []
    for ln in out.strip().splitlines():
        h, _, subj = ln.partition("\t")
        rows.append((h, subj))
    return rows


def load_at(ref):
    src = subprocess.check_output(
        ["git", "-C", TOOLDIR, "show",
         "%s:tools/influxdb-migration/pivot_lp.py" % ref],
        text=True,
    )
    tmp = os.path.join(HERE, "_pivot_traj_%s.py" % ref)
    with open(tmp, "w") as fh:
        fh.write(src)
    spec = importlib.util.spec_from_file_location("pivot_traj_%s" % ref, tmp)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod, tmp


def call_merge(mod, lines, interval):
    params = inspect.signature(mod.merge_stream).parameters
    feeder = NullFeeder()
    kwargs = {}
    if "coercer" in params:
        kwargs["coercer"] = None
    t0 = time.perf_counter()
    pts, fl = mod.merge_stream(iter(lines), "tm_", feeder, interval, **kwargs)
    return time.perf_counter() - t0, pts, fl


def best_of(mod, lines, interval, reps=4):
    best = None
    pts = fl = 0
    for _ in range(reps):
        dt, pts, fl = call_merge(mod, lines, interval)
        best = dt if best is None else min(best, dt)
    return best, pts, fl


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 3_000_000
    interval = 20_000_000_000
    print("python: %s" % sys.version.split()[0])
    print("generating %s field-lines...\n" % "{:,}".format(n))
    lines = gen_lines(n)

    rows = commits_touching()
    results = []
    base_pts = None
    for h, subj in rows:
        mod, tmp = load_at(h)
        try:
            dt, pts, fl = best_of(mod, lines, interval)
        finally:
            os.remove(tmp)
            if os.path.exists(tmp + "c"):
                os.remove(tmp + "c")
        if base_pts is None:
            base_pts = pts
        lps = int(fl / dt)
        flag = "" if pts == base_pts else "  [!! points=%d differ]" % pts
        results.append((h, subj, dt, lps, flag))

    fastest = max(r[3] for r in results)
    print("%-11s  %9s  %12s   %s" % ("commit", "secs", "lines/s", "subject"))
    print("-" * 90)
    for h, subj, dt, lps, flag in results:
        mark = "  <== fastest" if lps == fastest else ""
        print("%-11s  %8.3fs  %12s   %s%s%s" % (
            h, dt, "{:,}".format(lps), subj[:46], mark, flag))


if __name__ == "__main__":
    main()
