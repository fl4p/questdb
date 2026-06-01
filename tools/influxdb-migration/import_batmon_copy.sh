#!/bin/bash
# COPY-based re-import of the batmon InfluxDB bucket -- the COPY analog of
# import_batmon.sh.
#
# Versus the ILP path, COPY drops the WAL feed entirely: QuestDB's parallel COPY
# writes column files directly, bypassing the WAL sequencer, out-of-order
# partition rewrites, and the ILP-feed WAL-apply throttle. Pre-created tables
# carry DEDUP UPSERT KEYS so a resumed or re-run import is idempotent.
#
# The timestamp sort STAYS. The downsample pivot merges all fields of a
# (tagset, 20s-bucket) into one row using a single open bucket, which is only
# correct when the input is timestamp-sorted; the raw export-lp is series-major
# (each field's full time-series contiguous), so it MUST be sorted first.
# Verified on the real bucket: sorted input reproduces the production table
# exactly (9,696 = 9,696 for a 6h window); UNSORTED input silently produced ~12x
# too many fragmented rows -- pivot_lp now aborts loudly on unsorted input rather
# than corrupting. (COPY sorts each PARTITION internally, but that is for COPY's
# own column writes, not a substitute for the pivot's pre-merge sort.)
#
# Sort fix (same as import_batmon.sh): export-lp string fields can contain a
# space, so `sort -k3,3n` keys on the wrong token. The timestamp is always the
# LAST whitespace token -- prepend it, sort numerically by that, then strip it.
#
# Prerequisites on the QuestDB host:
#   * cairo.sql.copy.root set to a directory on a fast disk with room for the
#     staged wide CSVs plus COPY's per-partition temp index files. CSVs are
#     written under <copy-root>/<copy-subdir> and deleted after each table loads.
#   * tm-tables.sql declares each table (bulk_copy pre-creates them complete,
#     partitioned, with DEDUP UPSERT KEYS and a TIMESTAMP_NS designated column).
#   * Size `sort -S` to the host; on a small-RAM box do NOT co-schedule heavy
#     export/sort jobs with a live import (see FINDINGS-tm-import-and-pivot-perf.md).
set -o pipefail
cd ~/questdb/tools/influxdb-migration || exit 1

COPY_ROOT=${COPY_ROOT:-/var/lib/questdb/import}
QDB_URL=${QDB_URL:-http://localhost:9000}

echo "import started $(date -u +%FT%TZ)"
sudo -n /usr/bin/influxd inspect export-lp \
     --engine-path /mnt/HC_Vol32/influxdb/engine \
     --bucket-id 21bb6302e4d8bc07 --output-path - 2>/tmp/import-export.err \
  | grep -E '^(batmon|cells),' \
  | awk '{print $NF"\t"$0}' \
  | LC_ALL=C sort -S 1G --parallel=2 -T /mnt/HC_Vol32/bak/sorttmp -k1,1n \
  | cut -f2- \
  | python3 bulk_copy.py --from-stdin --assume-sorted \
        --prefix batmon_tele_ --downsample 20s \
        --schema-file tm-tables.sql \
        --copy-root "$COPY_ROOT" --copy-subdir batmon \
        --questdb-url "$QDB_URL"
rc=$?
echo "import finished $(date -u +%FT%TZ) rc=$rc"
