#!/usr/bin/env bash
set -xeuo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/init.sh"
BACKUP_NAME=$1
${CUR_DIR}/list.sh | grep "${BACKUP_NAME}" | while IFS= read -r line; do
  SNAPSHOT_ID=$(echo "${line}" | jq -r -c -M .snapshot_id)
  SNAPSHOT_PATH=$(echo "${line}" | jq -r -c -M .snapshot_path)
  kopia restore --parallel=$(nproc) "${SNAPSHOT_ID}" --skip-existing ${SNAPSHOT_PATH}
  LOCAL_BACKUP_DIR="$(dirname ${SNAPSHOT_PATH})/${BACKUP_NAME}"
  rm -rf "${LOCAL_BACKUP_DIR}"
  find "${SNAPSHOT_PATH}" -type f -name checksums.txt | parallel -j $(nproc) "${CUR_DIR}/checksum_parser.sh" {} "download" "${SNAPSHOT_PATH}"
  # need separately `rm` cause hash file can contains multiple the same files in different parts
  find ${SNAPSHOT_PATH} -maxdepth 1 -type f -regex '.*/[a-z0-9]\{32\}.*' | while read HASH_FILE; do
    rm "${SNAPSHOT_PATH}/${HASH_FILE}"
  done || true
  mv -f "${SNAPSHOT_PATH}" "${LOCAL_BACKUP_DIR}"
done
