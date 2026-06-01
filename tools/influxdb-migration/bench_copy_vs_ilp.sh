#!/bin/bash
# Benchmark: new CSV+COPY path (bulk_copy.py) vs old ILP path (pivot_lp.py),
# on the same staged, sorted batmon slice. Reports end-to-end and ingest-only
# rows/s. Run ON the QuestDB host (talks to localhost, writes the copy root).
set -o pipefail
cd ~/questdb/tools/influxdb-migration || exit 1
QURL=http://localhost:9000
SLICE=/tmp/bench_slice.lp
START=${START:-2026-05-28T00:00:00Z}
END=${END:-2026-05-28T01:00:00Z}
PY=${PY:-python3}            # interpreter for the pivot/producer (python3 or pypy3)
echo "interpreter: $($PY --version 2>&1)"

sql(){ curl -s -G "$QURL/exec" --data-urlencode "query=$1"; }
cnt(){ sql "SELECT count() FROM \"$1\"" | python3 -c "import sys,json
d=json.load(sys.stdin)
print(d['dataset'][0][0] if d.get('dataset') else 0)" 2>/dev/null || echo 0; }
rate(){ python3 -c "print(f'{$1/$2:,.0f}')"; }

# Stage the sorted slice once (export + sort excluded from the timings below).
if [ ! -f "$SLICE" ]; then
  echo "staging slice $START..$END"
  sudo -n /usr/bin/influxd inspect export-lp --engine-path /mnt/HC_Vol32/influxdb/engine \
     --bucket-id 21bb6302e4d8bc07 --measurement batmon \
     --start "$START" --end "$END" --output-path - 2>/dev/null \
   | grep -E '^batmon,' | awk '{print $NF"\t"$0}' \
   | LC_ALL=C sort -S 512M -T /tmp -k1,1n | cut -f2- > "$SLICE"
fi
echo "slice: $(wc -l < "$SLICE") field-lines"

# Schema files for each target table (clone of the production schema).
DDL=$(sql "SHOW CREATE TABLE batmon_tele_batmon" | python3 -c "import sys,json;print(json.load(sys.stdin)['dataset'][0][0])")
echo "$DDL" | sed 's/batmon_tele_batmon/bench_old_batmon/' > /tmp/schema_old.sql
echo "$DDL" | sed 's/batmon_tele_batmon/bench_new_batmon/' > /tmp/schema_new.sql

now(){ date +%s.%N; }
elapsed(){ python3 -c "print(f'{$2-$1:.1f}')"; }

# 1) Pivot-only baseline (no ingest): dry-run pivots + coerces, posts nothing.
t0=$(now)
cat "$SLICE" | $PY pivot_lp.py --prefix bench_old_ --downsample 20s \
    --schema-file /tmp/schema_old.sql --dry-run >/dev/null 2>&1
t1=$(now); PIVOT=$(elapsed "$t0" "$t1")
echo "pivot-only baseline: ${PIVOT}s"

# 2) OLD path: pivot + ILP-over-HTTP feed, then wait for WAL apply to finish.
sql "DROP TABLE IF EXISTS bench_old_batmon" >/dev/null
t0=$(now)
cat "$SLICE" | $PY pivot_lp.py --prefix bench_old_ --downsample 20s \
    --schema-file /tmp/schema_old.sql --questdb-url "$QURL" >/dev/null 2>&1
prev=-1; same=0
for i in $(seq 1 900); do c=$(cnt bench_old_batmon)
  if [ "$c" = "$prev" ]; then same=$((same+1)); [ $same -ge 2 ] && break; else same=0; fi
  prev=$c; sleep 1; done
t1=$(now); OLD=$(elapsed "$t0" "$t1"); ROLD=$(cnt bench_old_batmon)

# 3) NEW path: pivot + CSV + COPY (synchronous; bulk_copy polls to finished).
sql "DROP TABLE IF EXISTS bench_new_batmon" >/dev/null
docker exec questdb sh -c "mkdir -p /var/lib/questdb/import/bench && chmod 777 /var/lib/questdb/import/bench"
t0=$(now)
cat "$SLICE" | $PY bulk_copy.py --from-stdin --assume-sorted --prefix bench_new_ \
    --downsample 20s --schema-file /tmp/schema_new.sql \
    --copy-root /mnt/HC_Vol32/questdb/import --copy-subdir bench \
    --questdb-url "$QURL" >/dev/null 2>&1
t1=$(now); NEW=$(elapsed "$t0" "$t1"); RNEW=$(cnt bench_new_batmon)

echo
echo "================ RESULTS ($(wc -l < "$SLICE") field-lines) ================"
printf "%-12s %10s %10s %14s %16s\n" path secs rows "e2e rows/s" "ingest rows/s"
printf "%-12s %10s %10s %14s %16s\n" "OLD (ILP)" "$OLD" "$ROLD" "$(rate "$ROLD" "$OLD")" "$(rate "$ROLD" "$(python3 -c "print(max(0.1,$OLD-$PIVOT))")")"
printf "%-12s %10s %10s %14s %16s\n" "NEW (COPY)" "$NEW" "$RNEW" "$(rate "$RNEW" "$NEW")" "$(rate "$RNEW" "$(python3 -c "print(max(0.1,$NEW-$PIVOT))")")"
echo "(ingest rows/s = rows / (e2e - ${PIVOT}s pivot baseline); approximate)"
