#!/bin/bash
set -x
set -e

export CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
mkdir -p "${CUR_DIR}/_coverage_/"
rm -rf "${CUR_DIR}/_coverage_/*"

source "${CUR_DIR}/.env"

export CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION:-24.3}
if [[ "${CLICKHOUSE_VERSION}" =~ ^2[1-9]+ || "${CLICKHOUSE_VERSION}" == "head" ]]; then
  export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server}
else
  export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-yandex/clickhouse-server}
fi
export CLICKHOUSE_BACKUP_BIN="$(pwd)/clickhouse-backup/clickhouse-backup-race"
export LOG_LEVEL=${LOG_LEVEL:-info}
export TEST_LOG_LEVEL=${TEST_LOG_LEVEL:-info}

if [[ -f "${CUR_DIR}/credentials.json" ]]; then
  export GCS_TESTS=${GCS_TESTS:-1}
else
  export GCS_TESTS=${GCS_TESTS:-}
fi

export GLACIER_TESTS=${GLACIER_TESTS:-0}

export AZURE_TESTS=${AZURE_TESTS:-1}
export RUN_ADVANCED_TESTS=${RUN_ADVANCED_TESTS:-1}
export GODEBUG=${GODEBUG:-}
export S3_DEBUG=${S3_DEBUG:-false}
export GCS_DEBUG=${GCS_DEBUG:-false}
export FTP_DEBUG=${FTP_DEBUG:-false}
export SFTP_DEBUG=${SFTP_DEBUG:-false}
export AZBLOB_DEBUG=${AZBLOB_DEBUG:-false}
export CLICKHOUSE_DEBUG=${CLICKHOUSE_DEBUG:-false}

if [[ "${CLICKHOUSE_VERSION}" == 2* || "${CLICKHOUSE_VERSION}" == "head" ]]; then
  export COMPOSE_FILE=docker-compose_advanced.yml
else
  export COMPOSE_FILE=docker-compose.yml
fi


pids=()
for project in $(docker compose -f ${CUR_DIR}/${COMPOSE_FILE} ls --all -q); do
  docker compose -f ${CUR_DIR}/${COMPOSE_FILE} --project-name ${project} --progress plain down --remove-orphans --volumes --timeout=1 &
  pids+=($!)
done

for pid in "${pids[@]}"; do
  if wait "$pid"; then
      echo "$pid docker compose down successful"
  else
      echo "$pid docker compose down failed. Exiting."
      exit 1  # Exit with an error code if any command fails
  fi
done

docker volume prune -f
make clean build-race-docker build-race-fips-docker

export RUN_PARALLEL=${RUN_PARALLEL:-1}

docker compose -f ${CUR_DIR}/${COMPOSE_FILE} --progress=quiet pull

pids=()
for ((i = 0; i < RUN_PARALLEL; i++)); do
  docker compose -f ${CUR_DIR}/${COMPOSE_FILE} --project-name project${i} --progress plain up -d &
  pids+=($!)
done

for pid in "${pids[@]}"; do
  if wait "$pid"; then
      echo "$pid docker compose up successful"
  else
      echo "$pid docker compose up failed. Exiting."
      exit 1  # Exit with an error code if any command fails
  fi
done

go test -parallel ${RUN_PARALLEL} -race -timeout ${TEST_TIMEOUT:-60m} -failfast -tags=integration -run "${RUN_TESTS:-.+}" -v ${CUR_DIR}/integration_test.go
go tool covdata textfmt -i "${CUR_DIR}/_coverage_/" -o "${CUR_DIR}/_coverage_/coverage.out"

if [[ "1" == "${CLEAN_AFTER:-1}" ]]; then
  pids=()
  for project in $(docker compose -f ${CUR_DIR}/${COMPOSE_FILE} ls --all -q); do
    docker compose -f ${CUR_DIR}/${COMPOSE_FILE} --project-name ${project} --progress plain down --remove-orphans --volumes --timeout=1 &
    pids+=($!)
  done

  for pid in "${pids[@]}"; do
    if wait "$pid"; then
        echo "$pid docker compose down successful"
    else
        echo "$pid docker compose down failed. Exiting."
        exit 1  # Exit with an error code if any command fails
    fi
  done
fi