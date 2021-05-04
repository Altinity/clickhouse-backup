#!/bin/bash
set -x
set -e

export CLICKHOUSE_BACKUP_BIN="$(pwd)/clickhouse-backup/clickhouse-backup"
export LOG_LEVEL=${LOG_LEVEL:-info}

if [[ "${CLICKHOUSE_VERSION}" == 2* ]]; then
  export COMPOSE_FILE=docker-compose_storage-policy.yml
else
  export COMPOSE_FILE=docker-compose.yml
fi

docker-compose -f test/integration/${COMPOSE_FILE} down
make clean
make all
docker-compose -f test/integration/${COMPOSE_FILE} up -d --force-recreate

# To run integration tests including GCS tests set GCS_TESTS environment variable:
# GCS_TESTS=true go test -tags=integration -v test/integration/integration_test.go
go test -tags=integration -v test/integration/integration_test.go
