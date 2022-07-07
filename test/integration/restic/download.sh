#!/usr/bin/env bash
set -xeuo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/init.sh"
BACKUP_NAME=$1
SNAPSHOT_ID=$(${CUR_DIR}/list.sh | grep "${BACKUP_NAME}" | jq -r -c -M .snapshot_id)
DISKS_COUNT=$(clickhouse client -q "SELECT count() FROM system.disks FORMAT TSVRaw" || echo "1")
if [[ "1" != "${DISKS_COUNT}" ]]; then
  restic restore --tag "${BACKUP_NAME}" --target / "${SNAPSHOT_ID}"
else
  LOCAL_PATH=$(clickhouse client -q "SELECT concat(replaceRegexpOne(metadata_path,'/metadata.*$',''),'/backup') FROM system.tables WHERE database = 'system' AND metadata_path!='' LIMIT 1 FORMAT TSVRaw")
  restic restore --tag "${BACKUP_NAME}" --target "${LOCAL_PATH}" "${SNAPSHOT_ID}"
fi