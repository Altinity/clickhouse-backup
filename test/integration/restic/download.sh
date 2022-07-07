#!/usr/bin/env bash
set -xeuo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/init.sh"
BACKUP_NAME=$1
SNAPSHOT_ID=$(${CUR_DIR}/list.sh | grep "${BACKUP_NAME}" | jq -r -c -M .snapshot_id)
restic restore --verbose --tag "${BACKUP_NAME}" --target / "${SNAPSHOT_ID}"
