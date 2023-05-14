CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
mkdir -p "${CUR_DIR}/_coverage_/"
source "${CUR_DIR}/.env"
make clean build-race-docker
python3 "${CUR_DIR}/clickhouse_backup/regression.py" --debug --only=${RUN_TESTS:-*}"
