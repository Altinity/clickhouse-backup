CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/.env"
make build-race
python3 "${CUR_DIR}/clickhouse_backup/regression.py"
