#!/usr/bin/env bash
set -xeuo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/init.sh"
BACKUP_NAME=$1
DIFF_FROM_REMOTE=${2:-}
DIFF_FROM_REMOTE_CMD=""
LOCAL_PATHS=$(eval "clickhouse client $CLICKHOUSE_PARAMS -q \"SELECT concat(trim(TRAILING '/' FROM path),'/backup/','${BACKUP_NAME}') FROM system.disks FORMAT TSVRaw\" | awk '{printf(\"%s \",\$0)} END { printf \"\n\" }' || clickhouse client $CLICKHOUSE_PARAMS -q \"SELECT concat(replaceRegexpOne(metadata_path,'/metadata.*$',''),'/backup/','${BACKUP_NAME}') FROM system.tables WHERE database = 'system' AND metadata_path!='' LIMIT 1 FORMAT TSVRaw\" | awk '{printf(\"%s \",\$0)} END { printf \"\n\" }'")
if [[ "" != "${DIFF_FROM_REMOTE}" ]]; then
  DIFF_FROM_REMOTE_CMD="--parent ${DIFF_FROM_REMOTE}"
fi
SNAPSHOT_SOURCES=""
for dir in $(echo "${LOCAL_PATHS}"); do
  if [[ -d "${dir}" ]]; then
    UPLOAD_DIR="$(dirname "${dir}")/latest"
    rm -rf "${UPLOAD_DIR}"
    cp -rl "${dir}" "${UPLOAD_DIR}"
    find "${UPLOAD_DIR}" -type f -name checksums.txt | parallel -j $(nproc) "${CUR_DIR}/checksum_parser.sh" {} "upload" "${UPLOAD_DIR}"
    SNAPSHOT_SOURCES="${UPLOAD_DIR} ${SNAPSHOT_SOURCES}"
  fi
done

kopia snapshot create $DIFF_FROM_REMOTE_CMD  --parallel=$(nproc) --fail-fast --tags="backup_name:${BACKUP_NAME}"  $SNAPSHOT_SOURCES

for dir in $(echo "${LOCAL_PATHS}"); do
  if [[ -d "${dir}" ]]; then
    UPLOAD_DIR="$(dirname "${dir}")/latest"
    rm -rfv "${UPLOAD_DIR}"
  fi
done

