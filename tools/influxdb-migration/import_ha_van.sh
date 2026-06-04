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
set -euo pipefail
cd ~/questdb/tools/influxdb-migration || exit 1

ENGINE=${ENGINE:-/mnt/HC_Vol32/influxdb/engine}
SORTTMP=${SORTTMP:-/mnt/HC_Vol32/bak/sorttmp}
QDB_URL=${QDB_URL:-http://localhost:9000}
TARGET_MB=${TARGET_MB:-400}
BIN_MINUTES=${BIN_MINUTES:-60}
DOWNSAMPLE=${DOWNSAMPLE:-20s}   # applied to mppt only
# value-only: keep just the `value` field + entity_id/domain tags. HA writes
# the per-unit ("others") measurements as a single `value` field plus entity
# attribute fields/tags with arbitrary names (e.g. "Available (Important)") that
# are illegal QuestDB columns and break ingest; dropping them yields clean
# (timestamp, entity_id, domain, value) tables. VALUE_ONLY governs the OTHERS
# pass ONLY. The mppt pass is NEVER value-only: mppt is multi-field telemetry
# (voltage/current/power/...), so --value-only would drop every field but a
# (possibly absent) `value` -- catastrophic. mppt keeps all fields.
VALUE_ONLY=${VALUE_ONLY:-1}
RUN_MPPT=${RUN_MPPT:-1}       # set 0 to skip the (already-done) mppt pass
RUN_OTHERS=${RUN_OTHERS:-1}
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

# run_group <value-only:0|1> <bucket-id> <prefix> <downsample-or-empty> <--measurement m ...>
run_group() {
  local vonly="$1" bid="$2" prefix="$3" ds="$4"
  shift 4
  local margs=("$@")
  local san=(); [ "$vonly" = 1 ] && san=(--value-only)
  local dsflag=(); [ -n "$ds" ] && dsflag=(--downsample "$ds")

  # Plan the chunk ranges. A planner failure (bad engine path/bucket id, crash,
  # no TSM files) must NOT be mistaken for "no data": pipefail does not cover the
  # process substitution mapfile reads, so capture the exit code explicitly. On
  # failure mark the run failed rather than silently importing zero rows.
  local planfile prc=0 ranges
  planfile=$(mktemp)
  python3 tsm_chunk_plan.py --engine-path "$ENGINE" --bucket-id "$bid" \
      --target-mb "$TARGET_MB" --bin-minutes "$BIN_MINUTES" \
      ${margs[@]+"${margs[@]}"} > "$planfile" || prc=$?
  if [ "$prc" -ne 0 ]; then
    echo "!!! ${prefix} ds=${ds:-raw} tsm_chunk_plan FAILED rc=$prc -- NO DATA IMPORTED"
    # Record the failure and skip this group, but return 0 so `set -e` does not
    # abort the whole run at the call site -- other groups still run and the
    # final `exit $fail` reports the failure.
    fail=1; rm -f "$planfile"; return 0
  fi
  mapfile -t ranges < "$planfile"
  rm -f "$planfile"

  local n=${#ranges[@]}
  if [ "$n" -eq 0 ]; then echo "  (planner found no populated ranges)"; return 0; fi
  local i S E rc
  for ((i = 0; i < n; i++)); do
    S="${ranges[i]% *}"
    E="${ranges[i]#* }"
    echo ">>> ${prefix} ds=${ds:-raw} chunk $((i + 1))/$n: [$S, $E) $(date -u +%FT%TZ)"
    rc=0
    sudo -n /usr/bin/influxd inspect export-lp \
         --engine-path "$ENGINE" --bucket-id "$bid" ${margs[@]+"${margs[@]}"} \
         --start "$S" --end "$E" --output-path - 2>>/tmp/import-export.err \
      | python3 sanitize_lp.py ${san[@]+"${san[@]}"} 2>>/tmp/sanitize.err \
      | awk '{print $NF"\t"$0}' \
      | LC_ALL=C sort -S 1G --parallel=2 -T "$SORTTMP" -k1,1n \
      | cut -f2- \
      | "$PIVOT_PY" pivot_lp.py --prefix "$prefix" ${dsflag[@]+"${dsflag[@]}"} \
            --questdb-url "$QDB_URL" --max-pending-rows 10000000 || rc=$?
    [ "$rc" -ne 0 ] && { echo "!!! ${prefix} chunk $((i + 1)) FAILED rc=$rc"; fail=1; }
  done
  # Always succeed: a chunk failure is reported via the global `fail`, not the
  # function's return code. Without this, the loop's last `[ -ne ]` test (false
  # on a successful final chunk) would make run_group return 1 and `set -e`
  # would abort the script after a clean group.
  return 0
}

echo "ha_van import started $(date -u +%FT%TZ); pivot=$PIVOT_PY; mppt downsample=$DOWNSAMPLE"
for name in "${BUCKET_NAMES[@]}"; do
  bid="${BUCKET_IDS[$name]}"
  prefix="${name}_"
  echo "===== bucket $name -> ${prefix} ====="
  if [ "$RUN_MPPT" = 1 ]; then
    echo "--- mppt (downsample $DOWNSAMPLE, all fields) ---"
    run_group 0 "$bid" "$prefix" "$DOWNSAMPLE" --measurement mppt
  fi
  if [ "$RUN_OTHERS" = 1 ]; then
    echo "--- others (raw, value-only=$VALUE_ONLY) ---"
    margs=()
    for m in "${OTHERS[@]}"; do margs+=(--measurement "$m"); done
    run_group "$VALUE_ONLY" "$bid" "$prefix" "" "${margs[@]}"
  fi
  echo "  $name done; ${prefix}* tables now: $(tables "$prefix")"
done
echo "ha_van import finished $(date -u +%FT%TZ) overall_fail=$fail"
exit $fail
