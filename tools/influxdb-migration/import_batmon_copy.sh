#!/bin/bash
# COPY-based re-import of the batmon InfluxDB bucket -- the fast analog of
# import_batmon.sh.
#
# Versus the ILP path this drops the two per-run showstoppers:
#   * NO global `sort -S 1G` of the whole export. QuestDB's parallel COPY sorts
#     each partition by timestamp itself, so the pivot writes wide CSV in any
#     order (bulk_copy.py streams the export straight into csv_pivot.CsvSink).
#   * NO ILP feed / WAL-apply throttle. COPY writes column files directly,
#     bypassing the WAL sequencer and O3 partition rewrites.
#
# Prerequisites on the QuestDB host:
#   * cairo.sql.copy.root set to a directory on a fast disk with room for the
#     staged wide CSVs plus COPY's per-partition temp index files. CSVs are
#     written under <copy-root>/<copy-subdir> and deleted after each table loads.
#   * tm-tables.sql declares each table with a TIMESTAMP_NS designated timestamp
#     (bulk_copy pre-creates them complete + partitioned + DEDUP UPSERT KEYS).
#   * Run Stage 0 first on a sample: verify_series_major.sh confirms the export
#     is series-major so the no-global-sort pivot is safe. If it is NOT, add the
#     awk-prepend sort from import_batmon.sh ahead of bulk_copy (piped via
#     --from-stdin) -- COPY still sorts each partition, so the win holds.
#   * Size to the box: on a small-RAM host do NOT co-schedule heavy export/sort
#     jobs with a live import (see FINDINGS-tm-import-and-pivot-perf.md OOM note).
set -o pipefail
cd ~/questdb/tools/influxdb-migration || exit 1

COPY_ROOT=${COPY_ROOT:-/var/lib/questdb/import}
QDB_URL=${QDB_URL:-http://localhost:9000}

echo "import started $(date -u +%FT%TZ)"
sudo -n /usr/bin/influxd inspect export-lp \
     --engine-path /mnt/HC_Vol32/influxdb/engine \
     --bucket-id 21bb6302e4d8bc07 --output-path - 2>/tmp/import-export.err \
  | grep -E '^(batmon|cells),' \
  | python3 bulk_copy.py --from-stdin \
        --prefix batmon_tele_ --downsample 20s \
        --schema-file tm-tables.sql \
        --copy-root "$COPY_ROOT" --copy-subdir batmon \
        --questdb-url "$QDB_URL"
rc=$?
echo "import finished $(date -u +%FT%TZ) rc=$rc"
