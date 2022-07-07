#!/usr/bin/env bash
set -xeuo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/settings.sh"
BACKUP_NAME=$1
DIFF_FROM_REMOTE=${2:-}
DIFF_FROM_REMOTE_CMD=""
LOCAL_DISKS=$(clickhouse-client -q "SELECT concat(name, ':', trim(TRAILING '/' FROM path)) FROM system.disks FORMAT TSVRaw")
for disk in $LOCAL_DISKS; do
  disk_name=$(echo $disk | cut -d ":" -f 1)
  disk_path=$(echo $disk | cut -d ":" -f 2)
  if [[ -d "${disk_path}/backup/${BACKUP_NAME}/" ]]; then
    ssh -i "${BACKUP_SSH_KEY}" -o "StrictHostKeyChecking no" "${BACKUP_REMOTE_SERVER}" mkdir -pv "${BACKUP_REMOTE_DIR}/${BACKUP_NAME}/${disk_name}"
    if [[ "" != "${DIFF_FROM_REMOTE}" ]]; then
      DIFF_FROM_REMOTE_CMD="--link-dest '${BACKUP_REMOTE_DIR}/${DIFF_FROM_REMOTE}/${disk_name}'"
    fi
    rsync -e "ssh -i ${BACKUP_SSH_KEY} -o 'StrictHostKeyChecking no'" -avzrPH ${DIFF_FROM_REMOTE_CMD} "${disk_path}/backup/${BACKUP_NAME}/" "${BACKUP_REMOTE_SERVER}:${BACKUP_REMOTE_DIR}/${BACKUP_NAME}/${disk_name}"
  fi
done

if [[ "" != "${BACKUP_KEEP_TO_REMOTE}" && "0" != "${BACKUP_KEEP_TO_REMOTE}" ]]; then
  BACKUP_LIST=$(${CUR_DIR}/list.sh)
  BACKUP_COUNT=$(echo $BACKUP_LIST | wc -l)
  if [[ $BACKUP_COUNT > $BACKUP_KEEP_TO_REMOTE ]]; then
    let BACKUP_TO_NEED_DELETE=$BACKUP_COUNT - $BACKUP_KEEP_TO_REMOTE
    BACKUP_TO_NEED_DELETE=$(echo $BACKUP_LIST | clickhouse-local --input-format="JSONEachRow" -q "SELECT backup_name FROM table ORDER BY creation_date LIMIT ${BACKUP_TO_NEED_DELETE}")
    for backup_name in $BACKUP_TO_NEED_DELETE; do
      ${CUR_DIR}/delete.sh "${backup_name}"
    done
  fi
fi
