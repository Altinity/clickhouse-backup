#!/usr/bin/env bash
set +x
set -euo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/init.sh"
rm -rf /tmp/restic_list_full.json
restic snapshots --json | jq -c -M '.[] | {"snapshot_id": .short_id, "backup_name": .tags[0], "creation_date": .time }' > /tmp/restic_list.json
jq -c -r -M --slurp '.[].snapshot_id' /tmp/restic_list.json | while IFS= read -r snapshot_id ; do
  jq -c -M -s 'add' <(grep ${snapshot_id} /tmp/restic_list.json) <(restic stats --json ${snapshot_id}) >> /tmp/restic_list_full.json
done
cat /tmp/restic_list_full.json | jq -c -M --slurp '.[] | .data_size = .total_size | .metadata_size = 0'
set -x