#!/bin/bash
set -x
set -e

export CLICKHOUSE_BACKUP_BIN="$(pwd)/clickhouse-backup/clickhouse-backup"
LOG_LEVEL=debug
docker-compose -f test/integration/${COMPOSE_FILE:-docker-compose_storage-policy.yml} down
make clean
make all
docker-compose -f test/integration/${COMPOSE_FILE:-docker-compose_storage-policy.yml} up -d --force-recreate

# To run integration tests including GCS tests set GCS_TESTS environment variable:
# GCS_TESTS=true go test -tags=integration -v test/integration/integration_test.go
go test -tags=integration -v test/integration/integration_test.go
