#!/usr/bin/env bash
set +x
set -euo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/init.sh" &>/dev/null
kopia snapshot list --storage-stats --json | jq -c -M '.[] | {"snapshot_id": .id, "snapshot_path": .source.path, "backup_name": .tags["tag:backup_name"], "creation_date": .startTime, "upload_date": .endTime, "data_size": .storageStats.newData.packedContentBytes, "metadata_size": 0 }'
