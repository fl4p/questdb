#!/bin/bash
# Chunked re-import of the batmon InfluxDB bucket -- emits rows PROGRESSIVELY.
#
# Why chunked: the single-shot import_batmon.sh must sort the ENTIRE ~100 GB
# series-major export before the pivot can emit its first row -- the 20s
# downsample uses one open bucket and so needs GLOBALLY time-ordered input. So
# QuestDB shows 0 rows for ~40 min and the global sort spills ~60-90 GB to the
# sort temp dir (which OOM-killed an earlier run on this 7.6 GB box).
#
# This script instead exports the source in ASCENDING time ranges
# (export-lp --start/--end) and runs export | sort | pivot per range. Each
# range's sort is ~10-14x smaller than the global one, fits within sort -S, and
# drains fast, so rows appear after the FIRST range (~2-3 min) and grow per
# range. Feeding ascending ranges is an append (no O3 rewrite spiral).
#
# Correctness at the range boundaries: day boundaries land on exact 20s-bucket
# edges (86400 / 20 = 4320 buckets/day), so no bucket is split across two ranges.
# export-lp --end may be inclusive, so a point exactly on a boundary can appear
# in two ranges -- DEDUP UPSERT KEYS on the tables (enabled separately) make that
# boundary, and any whole-range re-run, idempotent.
#
# Granularity is DATA-DRIVEN, not a fixed time span. export-lp costs a FIXED
# ~80 s per call regardless of window, so uniform daily ranges would burn ~20 h
# of overhead -- almost all of it scanning empty days (this source has ~14-month
# deserts). And the source is wildly non-uniform in density (the 2023 history is
# sparse in downsampled ROWS but DENSE in field-lines -- 2023 sampled
# sub-second). Because the sort inside each range is itself blocking, first-row
# latency and peak sort-temp are set by the LARGEST range, so the ranges must be
# balanced by DATA VOLUME, not by time.
#
# tsm_chunk_plan.py reads only the TSM index (cheap) to histogram per-block
# compressed bytes, then cuts the populated span into ~TARGET_MB chunks. Empty
# time contributes no bytes, so a desert is absorbed into whichever chunk
# straddles it -- no export is ever spent purely on an empty span ("quickly skip
# times without points"). Each chunk's sort then fits in RAM and drains fast, so
# rows appear after the FIRST chunk (~2 min) and grow per chunk.
#
# Tunables via env: ENGINE, BUCKET, SORTTMP, QDB_URL, TARGET_MB (compressed MB
# per chunk; LP-expanded is ~40x), BIN_MINUTES (planner resolution).
set -o pipefail
cd ~/questdb/tools/influxdb-migration || exit 1

ENGINE=${ENGINE:-/mnt/HC_Vol32/influxdb/engine}
BUCKET=${BUCKET:-21bb6302e4d8bc07}
SORTTMP=${SORTTMP:-/mnt/HC_Vol32/bak/sorttmp}
QDB_URL=${QDB_URL:-http://localhost:9000}
TARGET_MB=${TARGET_MB:-500}
BIN_MINUTES=${BIN_MINUTES:-60}
mkdir -p "$SORTTMP"

rows() {
  curl -s -G "$QDB_URL/exec" --data-urlencode "query=SELECT count() FROM $1" \
    | grep -oE '\[\[[0-9]+' | grep -oE '[0-9]+'
}

# Ask the planner for volume-balanced, gap-skipping [start, end) ranges. It reads
# only the TSM index, so this is seconds. One line per range: "START END".
mapfile -t ranges < <(
  python3 tsm_chunk_plan.py --engine-path "$ENGINE" --bucket-id "$BUCKET" \
      --measurement batmon --measurement cells \
      --target-mb "$TARGET_MB" --bin-minutes "$BIN_MINUTES"
)
n=${#ranges[@]}
if [ "$n" -eq 0 ]; then
  echo "planner returned no ranges -- aborting"
  exit 1
fi

echo "chunked import started $(date -u +%FT%TZ); $n volume-balanced ranges (~${TARGET_MB} MB each)"

fail=0
for ((i = 0; i < n; i++)); do
  S="${ranges[i]% *}"
  E="${ranges[i]#* }"
  echo ">>> chunk $((i + 1))/$n: [$S, $E) start $(date -u +%FT%TZ)"
  sudo -n /usr/bin/influxd inspect export-lp \
       --engine-path "$ENGINE" --bucket-id "$BUCKET" \
       --measurement batmon --measurement cells \
       --start "$S" --end "$E" --output-path - 2>>/tmp/import-export.err \
    | awk '/^(batmon|cells),/{print $NF"\t"$0}' \
    | LC_ALL=C sort -S 1G --parallel=2 -T "$SORTTMP" -k1,1n \
    | cut -f2- \
    | python3 pivot_lp.py --prefix batmon_tele_ --downsample 20s \
          --schema-file tm-tables.sql --questdb-url "$QDB_URL" \
          --max-pending-rows 10000000
  rc=$?
  if [ $rc -ne 0 ]; then
    echo "!!! chunk $((i + 1)) FAILED rc=$rc (see /tmp/import-export.err)"
    fail=1
  fi
  echo "    done $(date -u +%FT%TZ) rc=$rc; batmon=$(rows batmon_tele_batmon) cells=$(rows batmon_tele_cells)"
done
echo "chunked import finished $(date -u +%FT%TZ) overall_fail=$fail"
exit $fail
