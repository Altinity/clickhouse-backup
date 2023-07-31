#!/usr/bin/env bash
export BACKUP_REMOTE_DIR="/root/rsync_backups/cluster/shard0"
export BACKUP_REMOTE_SERVER="root@sshd"
export BACKUP_SSH_KEY="/tmp/id_rsa"
export BACKUP_KEEP_TO_REMOTE=7
export CLICKHOUSE_PARAMS="--host '$(yq '.clickhouse.host' /etc/clickhouse-backup/config.yml)' --port '$(yq '.clickhouse.port' /etc/clickhouse-backup/config.yml)' --user '$(yq '.clickhouse.username' /etc/clickhouse-backup/config.yml)' --password '$(yq '.clickhouse.password' /etc/clickhouse-backup/config.yml)'"