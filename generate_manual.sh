#!/usr/bin/env bash
# make clean build-race
CLICKHOUSE_BACKUP_BIN=${CLICKHOUSE_BACKUP_BIN:-./clickhouse-backup/clickhouse-backup-race}
cmds=(
  tables
  create
  create_remote
  upload
  list
  download
  restore
  restore_remote
  delete
  default-config
  print-config
  clean
  clean_remote_broken
  watch
  server
)
for cmd in  ${cmds[@]}; do
  echo "### CLI command - ${cmd}"
  echo '```'
  "${CLICKHOUSE_BACKUP_BIN}" help "${cmd}"
  echo '```'
done