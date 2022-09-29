#!/usr/bin/env bash
set -xeuo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/init.sh"
BACKUP_NAME=$1
DIFF_FROM_REMOTE=${2:-}
DIFF_FROM_REMOTE_CMD=""
LOCAL_PATHS=$(clickhouse client -q "SELECT concat(trim(TRAILING '/' FROM path),'/backup/','${BACKUP_NAME}') FROM system.disks FORMAT TSVRaw" | awk '{printf("%s ",$0)} END { printf "\n" }' || clickhouse client -q "SELECT concat(replaceRegexpOne(metadata_path,'/metadata.*$',''),'/backup/','${BACKUP_NAME}') FROM system.tables WHERE database = 'system' AND metadata_path!='' LIMIT 1 FORMAT TSVRaw" | awk '{printf("%s ",$0)} END { printf "\n" }')
if [[ "" != "${DIFF_FROM_REMOTE}" ]]; then
#  DIFF_FROM_REMOTE_CMD="--parent ${DIFF_FROM_REMOTE}"
  DIFF_FROM_REMOTE_CMD=""
fi
SNAPSHOT_SOURCES=""
for dir in $(echo "${LOCAL_PATHS}"); do
  echo "${dir}"
  if [[ -d "${dir}" ]]; then
    SNAPSHOT_SOURCES="${dir} ${SNAPSHOT_SOURCES}"
  fi
done
kopia snapshot create $DIFF_FROM_REMOTE_CMD --fail-fast --tags="backup_name:${BACKUP_NAME}"  $SNAPSHOT_SOURCES
