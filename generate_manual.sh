#!/usr/bin/env bash
set -euo pipefail
CLICKHOUSE_BACKUP_BIN=${CLICKHOUSE_BACKUP_BIN:-build/$(go env GOOS)/$(go env GOARCH)/clickhouse-backup}
make clean "${CLICKHOUSE_BACKUP_BIN}" >&2

# Extract command list dynamically from `--help`. Skip the built-in `help, h` entry.
# New commands (e.g. `rebase`) need no changes here - they're picked up automatically.
mapfile -t cmds < <(
  go run ./cmd/clickhouse-backup/ --help 2>/dev/null \
    | awk '/^COMMANDS:/{flag=1; next} /^GLOBAL OPTIONS:/{flag=0} flag && NF{print $1}' \
    | sed 's/,$//' \
    | grep -vE '^(help|h)$'
)

manual_file=$(mktemp)
trap 'rm -f "${manual_file}"' EXIT

for cmd in "${cmds[@]}"; do
  echo "### CLI command - ${cmd}"
  echo '```'
  "${CLICKHOUSE_BACKUP_BIN}" help "${cmd}"
  echo '```'
done > "${manual_file}"

cp "${manual_file}" Manual.md

# README.md embeds the same content under "## Common CLI Usage" - keep it in sync so the two
# docs can't drift apart again.
readme_tmp=$(mktemp)
trap 'rm -f "${manual_file}" "${readme_tmp}"' EXIT
awk '/^## Common CLI Usage$/{print; print ""; exit} {print}' README.md > "${readme_tmp}"
cat "${manual_file}" >> "${readme_tmp}"
mv "${readme_tmp}" README.md

echo "Updated Manual.md and README.md's 'Common CLI Usage' section." >&2
