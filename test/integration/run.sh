#!/bin/bash
set -x
set -e

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
mkdir -p "${CUR_DIR}/_coverage_/"
rm -rf "${CUR_DIR}/_coverage_/*"
export CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION:-23.3}
if [[ "${CLICKHOUSE_VERSION}" =~ ^2[1-9]+ || "${CLICKHOUSE_VERSION}" == "head" ]]; then
  export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server}
else
  export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-yandex/clickhouse-server}
fi
export CLICKHOUSE_BACKUP_BIN="$(pwd)/clickhouse-backup/clickhouse-backup-race"
export LOG_LEVEL=${LOG_LEVEL:-info}

if [[ -f "${CUR_DIR}/credentials.json" ]]; then
  export GCS_TESTS=${GCS_TESTS:-1}
else
  export GCS_TESTS=${GCS_TESTS:-}
fi

export AZURE_TESTS=${AZURE_TESTS:-1}
export RUN_ADVANCED_TESTS=${RUN_ADVANCED_TESTS:-1}
export S3_DEBUG=${S3_DEBUG:-false}
export GCS_DEBUG=${GCS_DEBUG:-false}
export FTP_DEBUG=${FTP_DEBUG:-false}
export SFTP_DEBUG=${SFTP_DEBUG:-false}
export GODEBUG=${GODEBUG:-}
export CLICKHOUSE_DEBUG=${CLICKHOUSE_DEBUG:-false}

if [[ "${CLICKHOUSE_VERSION}" == 2* || "${CLICKHOUSE_VERSION}" == "head" ]]; then
  export COMPOSE_FILE=docker-compose_advanced.yml
else
  export COMPOSE_FILE=docker-compose.yml
fi

docker-compose -f ${CUR_DIR}/${COMPOSE_FILE} down --remove-orphans
docker volume prune -f
make clean build-race-docker
docker-compose -f ${CUR_DIR}/${COMPOSE_FILE} up -d
docker-compose -f ${CUR_DIR}/${COMPOSE_FILE} exec minio mc alias list

go test -timeout 30m -failfast -tags=integration -run "${RUN_TESTS:-.+}" -v ${CUR_DIR}/integration_test.go
go tool covdata textfmt -i "${CUR_DIR}/_coverage_/" -o "${CUR_DIR}/_coverage_/coverage.out"