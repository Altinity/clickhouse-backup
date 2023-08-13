#!/usr/bin/env bash
set -xeuo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/init.sh"
BACKUP_NAME=$1
LOCAL_DISKS=$(eval "clickhouse client $CLICKHOUSE_PARAMS -q \"SELECT concat(name, ':', trim(TRAILING '/' FROM path)) FROM system.disks FORMAT TSVRaw\" || clickhouse client $CLICKHOUSE_PARAMS -q \"SELECT concat('default:',replaceRegexpOne(metadata_path,'/metadata.*$','')) FROM system.tables WHERE database = 'system' AND metadata_path!='' LIMIT 1 FORMAT TSVRaw\"")
for disk in $LOCAL_DISKS; do
  disk_name=$(echo $disk | cut -d ":" -f 1)
  disk_path=$(echo $disk | cut -d ":" -f 2)
  mkdir -pv "${disk_path}/backup/${BACKUP_NAME}/"
  if [[ "0" != $(ssh -i ${BACKUP_SSH_KEY} -o 'StrictHostKeyChecking no' "${BACKUP_REMOTE_SERVER}" ls -d ${BACKUP_REMOTE_DIR}/${BACKUP_NAME}/*/ | grep -c "/${disk_name}/") ]]; then
    rsync -e "ssh -i ${BACKUP_SSH_KEY} -o 'StrictHostKeyChecking no'" -avzrPH "${BACKUP_REMOTE_SERVER}:${BACKUP_REMOTE_DIR}/${BACKUP_NAME}/${disk_name}/" "${disk_path}/backup/${BACKUP_NAME}"
  fi
done
