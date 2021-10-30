#!/bin/bash
set -x
set -e

export CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION:-21.8}
export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-yandex/clickhouse-server}
export CLICKHOUSE_BACKUP_BIN="$(pwd)/clickhouse-backup/clickhouse-backup-race"
export LOG_LEVEL=${LOG_LEVEL:-info}
export GCS_TESTS=${GCS_TESTS:-}
export AZURE_TESTS=${AZURE_TESTS:-}
export S3_DEBUG=${S3_DEBUG:-false}
export GCS_DEBUG=${GCS_DEBUG:-false}
export FTP_DEBUG=${FTP_DEBUG:-false}
export GODEBUG=${GODEBUG:-}
export CLICKHOUSE_DEBUG=${CLICKHOUSE_DEBUG:-false}

if [[ "${CLICKHOUSE_VERSION}" == 2* ]]; then
  export COMPOSE_FILE=docker-compose_advanced.yml
else
  export COMPOSE_FILE=docker-compose.yml
fi

docker-compose -f test/integration/${COMPOSE_FILE} down --remove-orphans
docker volume prune -f
make clean
make build-race
docker-compose -f test/integration/${COMPOSE_FILE} up -d --force-recreate
go test  -timeout 30m -failfast -tags=integration -run "${RUN_TESTS:-.+}" -v test/integration/integration_test.go
