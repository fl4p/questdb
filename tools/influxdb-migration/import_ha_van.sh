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
# auto-creates column types (no --schema-file).
#
# Incremental (--since auto|<RFC3339>): resume from the tables' newest data so a
# periodic re-run only ingests the tail. This needs DEDUP so the re-imported
# boundary overwrites instead of duplicating (QuestDB has no DELETE). The script
# enables DEDUP per table -- mppt keyed (timestamp, device), others keyed
# (timestamp, entity_id) -- before an incremental run and again after every run
# (so tables created this run are dedup-ready next time). With --since auto the
# watermark is computed PER GROUP (mppt vs others have different latest
# timestamps), backed up one margin: mppt one 20s bucket (the boundary bucket is
# recomputed and overwritten), others 60s of insurance. An explicit --since
# <RFC3339> applies the same watermark to all groups.
#
# Still volume-balanced + gap-skipping via tsm_chunk_plan.py (its --measurement
# filter matches the UNESCAPED name, so the unit measurements size correctly),
# timestamp-sorted, PyPy pivot.
set -euo pipefail
cd ~/questdb/tools/influxdb-migration || exit 1

# --since auto      -> incremental: resume from the tables' newest data per group
# --since <RFC3339> -> incremental from an explicit watermark (same for all groups)
# (absent)          -> full import
SINCE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --since) SINCE="${2:-}"; shift 2 ;;
    --since=*) SINCE="${1#--since=}"; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

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
MPPT_MARGIN_S=${MPPT_MARGIN_S:-20}     # back up one 20s downsample bucket
OTHERS_MARGIN_S=${OTHERS_MARGIN_S:-60} # raw: dedup-idempotent, cheap insurance
CUR_SINCE=""                  # per-group watermark, set by resolve_group_since
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

# Table names belonging to a bucket prefix, one per line. One bucket name is a
# strict prefix of another (ha_van_ vs ha_van_dn_), so a plain LIKE '<prefix>%'
# would also return the longer bucket's tables -- exclude any table that starts
# with a DIFFERENT bucket prefix that itself extends this one.
table_names() {
  local p="$1" t bn other
  for t in $(curl -s -G "$QDB_URL/exec" --data-urlencode \
      "query=SELECT table_name FROM tables() WHERE table_name LIKE '$p%'" \
      | grep -oE "\"$p[A-Za-z0-9_]*\"" | tr -d '"'); do
    local skip=0
    for bn in "${BUCKET_NAMES[@]}"; do
      other="${bn}_"
      [ "$other" = "$p" ] && continue
      case "$other" in "$p"*) case "$t" in "$other"*) skip=1 ;; esac ;; esac
    done
    [ "$skip" = 0 ] && echo "$t"
  done
}

# us-epoch max(timestamp) across a group of tables, or empty if none exist.
#   $1 prefix   $2 mode (mppt|others)
# mode=mppt selects only <prefix>mppt; mode=others selects all the rest.
group_watermark_us() {
  local prefix="$1" mode="$2" t sel=""
  for t in $(table_names "$prefix"); do
    if [ "$mode" = mppt ]; then [ "$t" = "${prefix}mppt" ] || continue
    else [ "$t" = "${prefix}mppt" ] && continue; fi
    [ -n "$sel" ] && sel="$sel UNION ALL "
    sel="${sel}SELECT max(timestamp) m FROM \"$t\""
  done
  [ -z "$sel" ] && return 0
  curl -s -G "$QDB_URL/exec" --data-urlencode \
    "query=SELECT cast(max(m) as long) FROM ($sel)" \
    | grep -oE '\[\[-?[0-9]+' | grep -oE '\-?[0-9]+'
}

# RFC3339 (UTC) for (us-epoch - margin_seconds). Runs on the Linux host.
us_minus_to_iso() {
  date -u -d "@$(( $1 / 1000000 - $2 ))" +%Y-%m-%dT%H:%M:%SZ
}

# Set CUR_SINCE for a group.  $1 prefix  $2 mode  $3 margin_seconds
#   SINCE=auto      -> per-group watermark (newest data minus margin); empty if
#                      the group has no tables yet (so it does a full import)
#   SINCE=<RFC3339> -> that literal watermark for every group
#   SINCE=""        -> empty (full import)
resolve_group_since() {
  local prefix="$1" mode="$2" margin="$3" wm
  CUR_SINCE=""
  if [ "$SINCE" = auto ]; then
    wm=$(group_watermark_us "$prefix" "$mode")
    if [ -n "$wm" ]; then
      CUR_SINCE=$(us_minus_to_iso "$wm" "$margin")
    else
      echo "  ($mode: no existing tables -> full import for this group)"
    fi
  elif [ -n "$SINCE" ]; then
    CUR_SINCE="$SINCE"
  fi
}

# Enable DEDUP on every current table for a prefix so re-imports are idempotent.
# mppt is keyed (timestamp, device); the others (timestamp, entity_id) -- entity_id
# functionally determines domain, and keying on it alone avoids failing on a table
# that happens to lack the domain column. Idempotent: re-enabling is a no-op. A
# table missing a key column (e.g. an untagged orphan) is logged and skipped.
ensure_dedup_all() {
  local prefix="$1" t keys resp
  for t in $(table_names "$prefix"); do
    if [ "$t" = "${prefix}mppt" ]; then keys="timestamp,device"
    else keys="timestamp,entity_id"; fi
    resp=$(curl -s -G "$QDB_URL/exec" --data-urlencode \
      "query=ALTER TABLE \"$t\" DEDUP ENABLE UPSERT KEYS($keys)")
    if echo "$resp" | grep -q '"error"'; then
      echo "  (dedup skip $t: $(echo "$resp" | grep -oE '"error":"[^"]*"'))"
    fi
  done
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
  local since_arg=(); [ -n "${CUR_SINCE:-}" ] && since_arg=(--since "$CUR_SINCE")
  local planfile prc=0 ranges
  planfile=$(mktemp)
  python3 tsm_chunk_plan.py --engine-path "$ENGINE" --bucket-id "$bid" \
      --target-mb "$TARGET_MB" --bin-minutes "$BIN_MINUTES" \
      ${since_arg[@]+"${since_arg[@]}"} ${margs[@]+"${margs[@]}"} > "$planfile" || prc=$?
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

echo "ha_van import started $(date -u +%FT%TZ); pivot=$PIVOT_PY; mppt downsample=$DOWNSAMPLE; since=${SINCE:-FULL}"
for name in "${BUCKET_NAMES[@]}"; do
  bid="${BUCKET_IDS[$name]}"
  prefix="${name}_"
  echo "===== bucket $name -> ${prefix} ====="
  # Incremental: the re-imported boundary must overwrite, not duplicate. Enable
  # DEDUP on the existing tables before exporting anything.
  [ -n "$SINCE" ] && ensure_dedup_all "$prefix"
  if [ "$RUN_MPPT" = 1 ]; then
    resolve_group_since "$prefix" mppt "$MPPT_MARGIN_S"
    echo "--- mppt (downsample $DOWNSAMPLE, all fields)${CUR_SINCE:+ since $CUR_SINCE} ---"
    run_group 0 "$bid" "$prefix" "$DOWNSAMPLE" --measurement mppt
  fi
  if [ "$RUN_OTHERS" = 1 ]; then
    resolve_group_since "$prefix" others "$OTHERS_MARGIN_S"
    echo "--- others (raw, value-only=$VALUE_ONLY)${CUR_SINCE:+ since $CUR_SINCE} ---"
    margs=()
    for m in "${OTHERS[@]}"; do margs+=(--measurement "$m"); done
    run_group "$VALUE_ONLY" "$bid" "$prefix" "" "${margs[@]}"
  fi
  # Prep future incrementals: enable DEDUP on any tables created this run too.
  ensure_dedup_all "$prefix"
  echo "  $name done; ${prefix}* tables now: $(tables "$prefix")"
done
echo "ha_van import finished $(date -u +%FT%TZ) overall_fail=$fail"
exit $fail
