#!/usr/bin/env bash
export BACKUP_REMOTE_DIR="/root/rsync_backups/cluster/shard0"
export BACKUP_REMOTE_SERVER="root@sshd"
export BACKUP_SSH_KEY="/tmp/id_rsa"
export BACKUP_KEEP_TO_REMOTE=7
export CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-custom-rsync.yml
export CLICKHOUSE_PARAMS="--host '$(yq '.clickhouse.host' ${CLICKHOUSE_BACKUP_CONFIG})' --port '$(yq '.clickhouse.port' ${CLICKHOUSE_BACKUP_CONFIG})' --user '$(yq '.clickhouse.username' ${CLICKHOUSE_BACKUP_CONFIG})' --password '$(yq '.clickhouse.password' ${CLICKHOUSE_BACKUP_CONFIG})'"