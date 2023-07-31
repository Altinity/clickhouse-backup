#!/usr/bin/env bash
set -xeuo pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/init.sh"
BACKUP_NAME=$1
ssh -i "${BACKUP_SSH_KEY}" -o "StrictHostKeyChecking no" "${BACKUP_REMOTE_SERVER}" rm -rf "${BACKUP_REMOTE_DIR}/${BACKUP_NAME}"