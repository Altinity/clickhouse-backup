#!/usr/bin/env bash
set -xeuo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/init.sh"
BACKUP_NAME=$1
snapshot_ids=$(kopia snapshot list --json --tags="backup_name:${BACKUP_NAME}" | jq -c -r -M '.[].id')
for snapshot_id in ${snapshot_ids}; do
  kopia snapshot delete "${snapshot_id}"
done
