#!/bin/bash
# Parallel-produce + serial-COPY backfill of the batmon InfluxDB bucket.
#
# Why this exists: the producer (the Python pivot) is the bottleneck of the
# backfill, not the ingest -- both the ILP and COPY paths are producer-bound, and
# the chunked importer (import_batmon_chunked.sh) runs ONE producer, chunks
# serial. The chunks the planner emits are time-disjoint, so producing them is
# embarrassingly parallel. Measured on this box, pivot_lp under PyPy holds ~139 MB
# RSS, so several producers fit the free RAM at once; a DuckDB pivot was no faster
# (parse-bound) and used ~14x the RAM (see export-lp-cost-model.md / the producer
# benchmark), so the lever for fastest backfill is PARALLELISM of pivot_lp, not a
# new engine. Measured parallel scaling on this 4-core box while it was ALSO
# running QuestDB and a concurrent import (load ~5): K=1 1.0x, K=2 1.49x, K=3
# 1.73x, K=4 2.18x aggregate. The knee is ~K=3 here; an idle box (all 4 cores
# free) would scale closer to linear. PAR defaults to 3 (one core left for
# QuestDB + the serial COPY); raise it toward 4 in a maintenance window.
#
# How it stays correct: each chunk's producer writes per-measurement wide CSVs;
# a single serial consumer (copy_chunk.py) COPYs them. COPY is O3-free and
# order-tolerant (ParallelCsvFileImporter sorts each partition itself), so the
# producers run wide while the consumer drains chunks in any order with no
# out-of-order partition rewrites. DEDUP UPSERT KEYS on the tables make a re-run
# or a boundary-straddling point idempotent.
#
# Pipeline: tsm_chunk_plan.py (index-only, volume-balanced, gap-skipping) -> for
# each chunk, in parallel up to PAR workers: export-lp --start/--end | sort |
# pivot_lp --csv-out-dir staging/chunk_i ; a background drainer COPYs each chunk
# as it becomes ready and deletes its CSVs. Producers stay bounded to PAR so the
# box (shared with QuestDB) is not oversubscribed.
#
# Tunables via env: ENGINE, BUCKET, SORTTMP, QDB_URL, COPY_ROOT, SUBDIR, TARGET_MB,
# BIN_MINUTES, PAR (producer parallelism), PIVOT_PY, SCHEMA, SORT_S, SORT_PARALLEL.
set -o pipefail
cd ~/questdb/tools/influxdb-migration || exit 1

SINCE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --since) SINCE="${2:-}"; shift 2 ;;
    --since=*) SINCE="${1#--since=}"; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

ENGINE=${ENGINE:-/mnt/HC_Vol32/influxdb/engine}
BUCKET=${BUCKET:-21bb6302e4d8bc07}
SORTTMP=${SORTTMP:-/mnt/HC_Vol32/bak/sorttmp}
QDB_URL=${QDB_URL:-http://localhost:9000}
COPY_ROOT=${COPY_ROOT:-/var/lib/questdb/import}
SUBDIR=${SUBDIR:-batmon_par}
TARGET_MB=${TARGET_MB:-500}
BIN_MINUTES=${BIN_MINUTES:-60}
PAR=${PAR:-3}                 # producer parallelism (box has 4 cores, shared with QuestDB)
SCHEMA=${SCHEMA:-tm-tables.sql}
SORT_S=${SORT_S:-1G}
SORT_PARALLEL=${SORT_PARALLEL:-2}
# PyPy gives the producer ~1.7-1.9x; resolve it even off a non-interactive PATH.
PIVOT_PY=${PIVOT_PY:-$(command -v pypy3 || echo "$HOME/.local/bin/pypy3")}
[ -x "$PIVOT_PY" ] || PIVOT_PY=python3

mkdir -p "$SORTTMP" "$COPY_ROOT/$SUBDIR"

rows() { curl -s "$QDB_URL/exec?query=select%20count()%20from%20$1" 2>/dev/null \
  | sed -E 's/.*"dataset":\[\[([0-9]+)\]\].*/\1/'; }

SINCE_ARG=()
if [ "$SINCE" = "auto" ]; then
  # Resume from the newest data already in the tables (max across both).
  newest=""
  for t in batmon_tele_batmon batmon_tele_cells; do
    v=$(curl -s "$QDB_URL/exec?query=select%20max(timestamp)%20from%20$t" 2>/dev/null \
      | sed -E 's/.*"dataset":\[\["?([^"]*)"?\]\].*/\1/')
    [ -n "$v" ] && [ "$v" \> "$newest" ] && newest="$v"
  done
  [ -n "$newest" ] && SINCE="$newest" || SINCE=""
  echo "incremental since (auto): ${SINCE:-<none, full>}"
fi
[ -n "$SINCE" ] && SINCE_ARG=(--since "$SINCE")

mapfile -t ranges < <(
  python3 tsm_chunk_plan.py --engine-path "$ENGINE" --bucket-id "$BUCKET" \
      --measurement batmon --measurement cells \
      --target-mb "$TARGET_MB" --bin-minutes "$BIN_MINUTES" "${SINCE_ARG[@]}"
)
n=${#ranges[@]}
if [ "$n" -eq 0 ]; then
  [ -n "$SINCE" ] && { echo "incremental: no new data"; exit 0; }
  echo "planner returned no ranges -- aborting"; exit 1
fi
echo "parallel import started $(date -u +%FT%TZ); since=${SINCE:-FULL}; $n chunks (~${TARGET_MB} MB); PAR=$PAR; producer=$PIVOT_PY"

produce_chunk() {
  local i="$1" S="$2" E="$3" dir="$COPY_ROOT/$SUBDIR/chunk_$1"
  rm -rf "$dir"; mkdir -p "$dir"
  if sudo -n /usr/bin/influxd inspect export-lp \
        --engine-path "$ENGINE" --bucket-id "$BUCKET" \
        --measurement batmon --measurement cells \
        --start "$S" --end "$E" --output-path - 2>>/tmp/par-export.err \
    | awk '/^(batmon|cells),/{print $NF"\t"$0}' \
    | LC_ALL=C sort -S "$SORT_S" --parallel="$SORT_PARALLEL" -T "$SORTTMP" -k1,1n \
    | cut -f2- \
    | "$PIVOT_PY" pivot_lp.py --csv-out-dir "$dir" --no-csv-schema-from-table \
          --schema-file "$SCHEMA" --prefix batmon_tele_ --downsample 20s \
          --csv-timestamp-mode epoch-ns >/dev/null 2>>/tmp/par-pivot.err
  then echo "ok" > "$dir/.ready"
  else echo "FAIL rc=$?" > "$dir/.ready"; fi
}

# Background drainer: COPY chunks in ascending order as each becomes ready.
# COPY is single-flight and order-tolerant, so serial drain never reorders
# partitions. The first chunk pre-creates the tables (idempotent).
drain_all() {
  local rc=0
  for ((i = 0; i < n; i++)); do
    local dir="$COPY_ROOT/$SUBDIR/chunk_$i"
    while [ ! -f "$dir/.ready" ]; do sleep 1; done
    if grep -q FAIL "$dir/.ready"; then
      echo "!!! chunk $i producer FAILED (see /tmp/par-*.err)"; rc=1; continue
    fi
    local mk=()
    [ "$i" -eq 0 ] && mk=(--create-tables --schema-file "$SCHEMA")
    if python3 copy_chunk.py --csv-dir "$dir" --copy-subdir "$SUBDIR/chunk_$i" \
        --prefix batmon_tele_ --questdb-url "$QDB_URL" "${mk[@]}"; then
      rm -rf "$dir"
      echo "    chunk $((i + 1))/$n COPYied $(date -u +%FT%TZ); batmon=$(rows batmon_tele_batmon) cells=$(rows batmon_tele_cells)"
    else
      echo "!!! chunk $i COPY FAILED"; rc=1
    fi
  done
  return $rc
}

drain_all & DRAIN_PID=$!

# Produce with bounded parallelism (PAR concurrent producers).
active=0
for ((i = 0; i < n; i++)); do
  S="${ranges[i]% *}"; E="${ranges[i]#* }"
  echo ">>> producing chunk $((i + 1))/$n: [$S, $E)"
  produce_chunk "$i" "$S" "$E" &
  active=$((active + 1))
  if [ "$active" -ge "$PAR" ]; then wait -n; active=$((active - 1)); fi
done
wait                       # remaining producers
wait "$DRAIN_PID"; fail=$?

echo "parallel import finished $(date -u +%FT%TZ) overall_fail=$fail"
exit $fail
