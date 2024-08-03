#!/usr/bin/env bash
set +x
set -euo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/init.sh"
ssh -i "${BACKUP_SSH_KEY}" -o "StrictHostKeyChecking no" "${BACKUP_REMOTE_SERVER}" ls -d -1 "${BACKUP_REMOTE_DIR}/*" 2>/dev/null | while IFS= read -r backup_name ; do
  backup_name=${backup_name#"$BACKUP_REMOTE_DIR"}
  ssh -i "${BACKUP_SSH_KEY}" -o "StrictHostKeyChecking no" "${BACKUP_REMOTE_SERVER}" cat "${BACKUP_REMOTE_DIR}/${backup_name}/default/metadata.json" 2>/dev/null | jq -c -r -M '. + {upload_date: .creation_date}'
done
set -x
