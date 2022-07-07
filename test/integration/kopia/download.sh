#!/usr/bin/env bash
set -xeuo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/init.sh"
BACKUP_NAME=$1
${CUR_DIR}/list.sh | grep "${BACKUP_NAME}" | while IFS= read -r line; do
  SNAPSHOT_ID=$(echo "${line}" | jq -r -c -M .snapshot_id)
  SNAPSHOT_PATH=$(echo "${line}" | jq -r -c -M .snapshot_path)
  kopia restore "${SNAPSHOT_ID}"  --skip-existing ${SNAPSHOT_PATH}
done
