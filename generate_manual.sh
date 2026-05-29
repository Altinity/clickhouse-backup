#!/usr/bin/env bash
set -euo pipefail
CLICKHOUSE_BACKUP_BIN=${CLICKHOUSE_BACKUP_BIN:-build/$(go env GOOS)/$(go env GOARCH)/clickhouse-backup}
make clean "${CLICKHOUSE_BACKUP_BIN}" >&2

# Extract command list dynamically from `--help`. Skip the built-in `help, h` entry.
mapfile -t cmds < <(
  go run ./cmd/clickhouse-backup/ --help 2>/dev/null \
    | awk '/^COMMANDS:/{flag=1; next} /^GLOBAL OPTIONS:/{flag=0} flag && NF{print $1}' \
    | sed 's/,$//' \
    | grep -vE '^(help|h)$'
)

for cmd in "${cmds[@]}"; do
  echo "### CLI command - ${cmd}"
  echo '```'
  "${CLICKHOUSE_BACKUP_BIN}" help "${cmd}"
  echo '```'
done
