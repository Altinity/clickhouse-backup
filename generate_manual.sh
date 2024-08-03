#!/usr/bin/env bash
CLICKHOUSE_BACKUP_BIN=${CLICKHOUSE_BACKUP_BIN:-build/linux/$(dpkg --print-architecture)/clickhouse-backup}
make clean ${CLICKHOUSE_BACKUP_BIN}
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
