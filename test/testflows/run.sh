CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
mkdir -p "${CUR_DIR}/_coverage_/"
rm -rf "${CUR_DIR}/_coverage_/*"
source "${CUR_DIR}/.env"
export CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION:-25.8}
if [[ "${CLICKHOUSE_VERSION}" =~ ^2[1-9]+ || "${CLICKHOUSE_VERSION}" == "head" ]]; then
  export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server}
else
  export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-yandex/clickhouse-server}
fi
make clean build-race-docker
python3 "${CUR_DIR}/clickhouse_backup/regression.py" --debug --only="${RUN_TESTS:-*}"
go tool covdata textfmt -i "${CUR_DIR}/_coverage_/" -o "${CUR_DIR}/_coverage_/coverage.out"
docker buildx prune -f --filter=until=30m --max-used-space=1G
