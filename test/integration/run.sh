#!/bin/bash
set -x
set -e

export CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION:-22.3}
if [[ "${CLICKHOUSE_VERSION}" =~ 2[2-9]+ ]]; then
  export CLICKHOUSE_IMAGE=clickhouse/clickhouse-server
else
  export CLICKHOUSE_IMAGE=yandex/clickhouse-server
fi
export CLICKHOUSE_BACKUP_BIN="$(pwd)/clickhouse-backup/clickhouse-backup-race"
export LOG_LEVEL=${LOG_LEVEL:-info}
export GCS_TESTS=${GCS_TESTS:-}
export AZURE_TESTS=${AZURE_TESTS:-}
export RUN_ADVANCED_TESTS=${RUN_ADVANCED_TESTS:-}
export S3_DEBUG=${S3_DEBUG:-false}
export GCS_DEBUG=${GCS_DEBUG:-false}
export FTP_DEBUG=${FTP_DEBUG:-false}
export SFTP_DEBUG=${SFTP_DEBUG:-false}
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
if [[ "${COMPOSE_FILE}" == "docker-compose_advanced.yml" ]]; then
  docker-compose -f test/integration/${COMPOSE_FILE} up -d minio mysql
else
  docker-compose -f test/integration/${COMPOSE_FILE} up -d minio
fi
sleep 5
docker-compose -f test/integration/${COMPOSE_FILE} exec minio mc alias list

docker-compose -f test/integration/${COMPOSE_FILE} up -d
go test -timeout 30m -failfast -tags=integration -run "${RUN_TESTS:-.+}" -v test/integration/integration_test.go
