#!/usr/bin/env bash
#
# verify_series_major.sh -- Stage 0 gate for the COPY import path.
#
# The COPY pipeline (bulk_copy.py) pivots with BOUNDED per-tagset memory and NO
# global sort: it merges all field-lines of one (measurement + tag set) -- the LP
# "head", everything before the first space -- while they are adjacent, then
# flushes when the head changes. That is only correct if the export emits every
# field-line of a head CONTIGUOUSLY (series-major). InfluxDB's TSM export usually
# does, because the field name is part of the series key, but this is the
# load-bearing assumption, so verify it on real data before relying on it.
#
# This scans the export and reports any head that REAPPEARS after the stream had
# moved on to a different head (i.e. is not contiguous). If it reports none, the
# no-global-sort pivot is safe. If it reports some, keep an upstream sort (e.g.
#   awk '{print $NF"\t"$0}' | LC_ALL=C sort -k1,1n | cut -f2-
# ) ahead of the pivot -- COPY still sorts each partition itself, so you keep the
# main win regardless.
#
# Usage:
#   verify_series_major.sh export.lp
#   influxd inspect export-lp ... --output-path - | verify_series_major.sh
#
# Limitation: the head is taken as the bytes up to the first SPACE. A measurement
# or tag value containing an escaped space (rare in the numeric telemetry this
# targets) would be mis-split; the check is a heuristic, not a parser.

set -euo pipefail

awk '
{
  # Skip blank and comment lines.
  if ($0 == "" || substr($0, 1, 1) == "#") next
  sp = index($0, " ")
  if (sp == 0) next                  # no field set; ignore
  head = substr($0, 1, sp - 1)
  total++
  if (head != prev) {
    if (head in seen) {
      bad++
      if (bad <= 10) print "NON-CONTIGUOUS head reappears: " head
    }
    seen[head] = 1
    prev = head
    heads++
  }
}
END {
  printf "scanned %d field-lines across %d distinct head-runs\n", total, heads
  if (bad > 0) {
    printf "RESULT: NOT series-major -- %d head(s) reappear non-contiguously.\n", bad
    print  "        Keep an upstream sort ahead of the pivot (see header)."
    exit 1
  }
  print "RESULT: series-major -- every head is contiguous. No global sort needed."
}
' "${1:-/dev/stdin}"
