#!/usr/bin/env bash
CLICKHOUSE_BACKUP_BIN=${CLICKHOUSE_BACKUP_BIN:-build/$(go env GOOS)/$(go env GOARCH)/clickhouse-backup}
make clean ${CLICKHOUSE_BACKUP_BIN} >&2
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
  clean_local_broken
  clean_broken_retention
  watch
  acvp
  server
)
for cmd in  ${cmds[@]}; do
  echo "### CLI command - ${cmd}"
  echo '```'
  "${CLICKHOUSE_BACKUP_BIN}" help "${cmd}"
  echo '```'
done
