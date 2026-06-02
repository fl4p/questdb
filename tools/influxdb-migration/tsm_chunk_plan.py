#!/usr/bin/env python3
"""Plan volume-balanced, gap-skipping time chunks for an InfluxDB->QuestDB import.

The bulk importer (import_batmon_chunked.sh) runs ``export-lp | sort | pivot``
per time range. The sort inside each range is blocking, so first-row latency and
peak sort-temp are set by the LARGEST range -- bounding that sort is the whole
point of chunking. A fixed time span is a poor unit: the source is wildly
non-uniform in density (InfluxDB telemetry here is ~700 MB compressed across two
dense months in 2023 plus ~2 GB in a single recent week, with ~14-month empty
deserts between), so equal time spans yield wildly unequal sorts. (export-lp's
own startup is cheap -- ~1 s measured -- so this is about the sort, not export
overhead; see export-lp-cost-model.md.)

This planner reads only the TSM *index* (``influxd inspect dump-tsm --index``,
no block decode -- seconds, not minutes) to build a fine time histogram of
per-block compressed byte size, then greedily cuts it into chunks each holding
about ``--target-mb`` compressed bytes. Empty bins contribute nothing, so a gap
is simply absorbed into whichever chunk straddles it -- no export is ever spent
on an empty span. Compressed size (not point count) is the unit because it is
free from the index and tracks both the export stream and the sort cost; the
LP-expanded volume is a roughly constant multiple of it.

It enumerates the bucket's TSM files via ``influxd inspect report-tsm`` so it
needs no directory listing (the engine dir is root-owned). Both inspect calls go
through ``sudo -n`` to match the import scripts' NOPASSWD rule.

Output (stdout): one ``START_RFC3339 END_RFC3339`` pair per line, ascending,
contiguous, covering exactly the populated span. A human-readable plan with the
estimated bytes per chunk goes to stderr.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from bulk_v1 import _measurement_end, _unescape_measurement


def _run(cmd: List[str]) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc.stdout


def list_tsm_files(engine_path: str, bucket_id: str) -> List[Tuple[str, str, str, str]]:
    """Return (replication_policy, shard, file, max_time) rows via report-tsm."""
    out = _run([
        "sudo", "-n", "/usr/bin/influxd", "inspect", "report-tsm",
        "--data-path", f"{engine_path}/data", "--pattern", bucket_id,
    ])
    rows: List[Tuple[str, str, str, str]] = []
    for line in out.splitlines():
        parts = line.split()
        # Data rows: DB RP Shard File Series New(est) MinTime MaxTime LoadTime
        if len(parts) >= 8 and parts[0] == bucket_id:
            rows.append((parts[1], parts[2], parts[3], parts[7]))
    return rows


def parse_ts(s: str) -> datetime:
    # RFC3339 like 2023-11-08T09:58:55.772Z (variable fractional digits).
    s = s.rstrip("Z")
    if "." in s:
        head, frac = s.split(".")
        frac = (frac + "000000")[:6]
        s = f"{head}.{frac}"
        fmt = "%Y-%m-%dT%H:%M:%S.%f"
    else:
        fmt = "%Y-%m-%dT%H:%M:%S"
    return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)


def build_histogram(
    engine_path: str,
    bucket_id: str,
    measurements: Tuple[str, ...],
    bin_seconds: int,
    since_epoch: Optional[int] = None,
) -> Dict[int, int]:
    """Map bin-start-epoch -> summed compressed block bytes, over the index only.

    With ``since_epoch`` set (incremental mode), files whose whole time range
    predates it are skipped without opening them, and bins entirely before it are
    dropped -- so the planner reads and plans only the newer tail.
    """
    files = list_tsm_files(engine_path, bucket_id)
    if not files:
        raise SystemExit(f"no TSM files found for bucket {bucket_id}")
    want = set(measurements)  # exact, unescaped measurement names
    hist: Dict[int, int] = {}
    for rp, shard, fname, max_time in files:
        if since_epoch is not None:
            try:
                if int(parse_ts(max_time).timestamp()) < since_epoch:
                    continue  # whole file predates the watermark
            except ValueError:
                pass
        path = f"{engine_path}/data/{bucket_id}/{rp}/{shard}/{fname}"
        out = _run([
            "sudo", "-n", "/usr/bin/influxd", "inspect", "dump-tsm",
            "--index", "--file-path", path,
        ])
        for line in out.splitlines():
            cols = line.split("\t")
            # Index rows: Pos, MinTime, MaxTime, Ofs, Size, Key, Field
            if len(cols) < 6 or "T" not in cols[1]:
                continue
            key = cols[5]
            # The Key is the escaped "measurement,tags" series key; match on the
            # UNESCAPED measurement so names with spaces/specials (e.g. HA's
            # "% available", "kWh/d") filter correctly, not just clean prefixes.
            if want:
                meas = _unescape_measurement(key[:_measurement_end(key)])
                if meas not in want:
                    continue
            try:
                epoch = int(parse_ts(cols[1]).timestamp())
                size = int(cols[4])
            except (ValueError, IndexError):
                continue
            b = epoch - (epoch % bin_seconds)
            if since_epoch is not None and b + bin_seconds <= since_epoch:
                continue  # bin entirely before the watermark
            hist[b] = hist.get(b, 0) + size
    return hist


def plan_chunks(
    hist: Dict[int, int], target_bytes: int, bin_seconds: int,
    since_epoch: Optional[int] = None,
) -> List[Tuple[datetime, datetime, int]]:
    """Greedy: accumulate bins in time order; cut once a chunk reaches target.

    Empty time contributes no bins, so a gap is absorbed into the chunk that
    spans it -- no chunk is ever spent purely on empty time. With ``since_epoch``
    the first chunk starts exactly at the watermark (not its bin boundary), so
    the incremental export re-does at most one partial bin (idempotent via DEDUP).
    """
    if not hist:
        return []
    bins = sorted(hist)
    chunks: List[Tuple[datetime, datetime, int]] = []
    start = bins[0] if since_epoch is None else max(bins[0], since_epoch)
    acc = 0
    for b in bins:
        acc += hist[b]
        if acc >= target_bytes:
            end = b + bin_seconds  # close at the end of this bin
            chunks.append((
                datetime.fromtimestamp(start, timezone.utc),
                datetime.fromtimestamp(end, timezone.utc),
                acc,
            ))
            start = end
            acc = 0
    if acc > 0:  # trailing remainder
        end = bins[-1] + bin_seconds
        chunks.append((
            datetime.fromtimestamp(start, timezone.utc),
            datetime.fromtimestamp(end, timezone.utc),
            acc,
        ))
    return chunks


def fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--engine-path", required=True)
    p.add_argument("--bucket-id", required=True)
    p.add_argument(
        "--measurement", action="append", default=[],
        help="restrict the histogram to these measurement(s); repeatable",
    )
    p.add_argument(
        "--target-mb", type=float, default=150.0,
        help="target COMPRESSED MB per chunk (LP-expanded is ~40x this); "
        "default 150 MB ~= a few GB of sort temp per chunk",
    )
    p.add_argument(
        "--bin-minutes", type=int, default=60,
        help="histogram resolution; also the minimum chunk granularity",
    )
    p.add_argument(
        "--since", default=None,
        help="incremental: only plan data at/after this RFC3339 watermark "
        "(skips older files/bins; first chunk starts exactly here)",
    )
    args = p.parse_args(argv)

    bin_seconds = args.bin_minutes * 60
    since_epoch = int(parse_ts(args.since).timestamp()) if args.since else None
    hist = build_histogram(
        args.engine_path, args.bucket_id, tuple(args.measurement), bin_seconds,
        since_epoch,
    )
    target_bytes = int(args.target_mb * 1024 * 1024)
    chunks = plan_chunks(hist, target_bytes, bin_seconds, since_epoch)

    total = sum(hist.values())
    sys.stderr.write(
        f"plan: {len(chunks)} chunks over {total / 1048576:.1f} MB compressed "
        f"({len(hist)} non-empty {args.bin_minutes}-min bins), "
        f"target {args.target_mb:.0f} MB/chunk\n"
    )
    for i, (s, e, b) in enumerate(chunks):
        span = e - s
        sys.stderr.write(
            f"  chunk {i + 1:2d}: {fmt(s)} .. {fmt(e)}  "
            f"({b / 1048576:7.1f} MB, span {span})\n"
        )
        print(f"{fmt(s)} {fmt(e)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
