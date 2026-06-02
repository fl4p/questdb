#!/bin/bash
# Copy the Home Assistant buckets (ha_van, ha_van_dn) into QuestDB.
#
# HA uses the unit as the measurement name (%, kWh/d, degC, ...), illegal as a
# QuestDB table name. sanitize_lp.py rewrites each to an approved identifier and
# drops the skip-list (the untagged 'ºC' orphan); the pivot then adds the
# '<bucket>_' prefix. One table per (bucket, sanitized measurement).
#
# Downsample policy: mppt is sub-second (~2.75B points over 4.6 years) so it is
# downsampled to a 20s grid; every other measurement is loaded RAW (exact pivot).
# Implemented as two passes per bucket so each gets its own --downsample. ILP
# auto-creates column types (no --schema-file). No DEDUP keys (auto-created
# tables) -> this is a FULL one-shot load; add DEDUP before any incremental re-run.
#
# Still volume-balanced + gap-skipping via tsm_chunk_plan.py (its --measurement
# filter matches the UNESCAPED name, so the unit measurements size correctly),
# timestamp-sorted, PyPy pivot.
set -o pipefail
cd ~/questdb/tools/influxdb-migration || exit 1

ENGINE=${ENGINE:-/mnt/HC_Vol32/influxdb/engine}
SORTTMP=${SORTTMP:-/mnt/HC_Vol32/bak/sorttmp}
QDB_URL=${QDB_URL:-http://localhost:9000}
TARGET_MB=${TARGET_MB:-500}
BIN_MINUTES=${BIN_MINUTES:-60}
DOWNSAMPLE=${DOWNSAMPLE:-20s}   # applied to mppt only
mkdir -p "$SORTTMP"

BUCKET_NAMES=(ha_van ha_van_dn)
declare -A BUCKET_IDS=( [ha_van]=2480772d801b9374 [ha_van_dn]=32d1e9dca26087c9 )

# All measurements EXCEPT mppt (separate downsampled pass) and ºC (skip-listed).
OTHERS=(
  A Ah B GB GiB N V W Wh batmon cells dB dBm hPa kW kWh km lx m min ms packets
  psi s smart_shunt state steps
  "%" "% available" "%/d" "F/m" "KiB/s" "kWh/d" "kWh/h" "km/h" "packets/s"
  "pending update(s)" "°C"
)

if [ -z "${PIVOT_PY:-}" ]; then
  if command -v pypy3 >/dev/null 2>&1; then PIVOT_PY=pypy3
  elif [ -x "$HOME/.local/bin/pypy3" ]; then PIVOT_PY="$HOME/.local/bin/pypy3"
  else PIVOT_PY=python3; fi
fi

tables() {
  curl -s -G "$QDB_URL/exec" --data-urlencode \
    "query=SELECT count() FROM tables() WHERE table_name LIKE '$1%'" \
    | grep -oE '\[\[[0-9]+' | grep -oE '[0-9]+'
}

fail=0

# run_group <bucket-id> <prefix> <downsample-or-empty> <--measurement m ...>
run_group() {
  local bid="$1" prefix="$2" ds="$3"
  shift 3
  local margs=("$@")
  local dsflag=()
  [ -n "$ds" ] && dsflag=(--downsample "$ds")
  mapfile -t ranges < <(
    python3 tsm_chunk_plan.py --engine-path "$ENGINE" --bucket-id "$bid" \
        --target-mb "$TARGET_MB" --bin-minutes "$BIN_MINUTES" "${margs[@]}"
  )
  local n=${#ranges[@]}
  if [ "$n" -eq 0 ]; then echo "  (no data) "; return 0; fi
  local i S E rc
  for ((i = 0; i < n; i++)); do
    S="${ranges[i]% *}"
    E="${ranges[i]#* }"
    echo ">>> ${prefix} ds=${ds:-raw} chunk $((i + 1))/$n: [$S, $E) $(date -u +%FT%TZ)"
    sudo -n /usr/bin/influxd inspect export-lp \
         --engine-path "$ENGINE" --bucket-id "$bid" "${margs[@]}" \
         --start "$S" --end "$E" --output-path - 2>>/tmp/import-export.err \
      | python3 sanitize_lp.py 2>>/tmp/sanitize.err \
      | awk '{print $NF"\t"$0}' \
      | LC_ALL=C sort -S 1G --parallel=2 -T "$SORTTMP" -k1,1n \
      | cut -f2- \
      | "$PIVOT_PY" pivot_lp.py --prefix "$prefix" "${dsflag[@]}" \
            --questdb-url "$QDB_URL" --max-pending-rows 10000000
    rc=$?
    [ $rc -ne 0 ] && { echo "!!! ${prefix} chunk $((i + 1)) FAILED rc=$rc"; fail=1; }
  done
}

echo "ha_van import started $(date -u +%FT%TZ); pivot=$PIVOT_PY; mppt downsample=$DOWNSAMPLE"
for name in "${BUCKET_NAMES[@]}"; do
  bid="${BUCKET_IDS[$name]}"
  prefix="${name}_"
  echo "===== bucket $name -> ${prefix} ====="
  echo "--- mppt (downsample $DOWNSAMPLE) ---"
  run_group "$bid" "$prefix" "$DOWNSAMPLE" --measurement mppt
  echo "--- others (raw) ---"
  margs=()
  for m in "${OTHERS[@]}"; do margs+=(--measurement "$m"); done
  run_group "$bid" "$prefix" "" "${margs[@]}"
  echo "  $name done; ${prefix}* tables now: $(tables "$prefix")"
done
echo "ha_van import finished $(date -u +%FT%TZ) overall_fail=$fail"
exit $fail
