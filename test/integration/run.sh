#!/bin/bash
set -x
set -e

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
export CUR_DIR
mkdir -p "${CUR_DIR}/_coverage_/"
rm -rf "${CUR_DIR}/_coverage_/*"

source "${CUR_DIR}/.env"

export CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION:-25.8}
if [[ "${CLICKHOUSE_VERSION}" =~ ^2[1-9]+ || "${CLICKHOUSE_VERSION}" == "head" ]]; then
  export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server}
else
  export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-yandex/clickhouse-server}
fi
CLICKHOUSE_BACKUP_BIN="$(pwd)/clickhouse-backup/clickhouse-backup-race"
export CLICKHOUSE_BACKUP_BIN
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
export COS_DEBUG=${COS_DEBUG:-false}
export CLICKHOUSE_DEBUG=${CLICKHOUSE_DEBUG:-false}

if [[ "${CLICKHOUSE_VERSION}" == 2* || "${CLICKHOUSE_VERSION}" == "head" ]]; then
  export COMPOSE_FILE=docker-compose_advanced.yml
else
  export COMPOSE_FILE=docker-compose.yml
fi

for id in $(docker ps -q); do
  docker stop "${id}" --timeout 1
  docker rm -f "${id}"
done

pids=()
project_ids=()
for project in $(docker compose -f "${CUR_DIR}/${COMPOSE_FILE}" ls --all -q); do
  docker compose -f "${CUR_DIR}/${COMPOSE_FILE}" --project-name "${project}" --progress plain down --remove-orphans --volumes --timeout=1 &
  pids+=($!)
  project_ids+=("${project}")
done

for index in "${!pids[@]}"; do
  pid=${pids[index]}
  project_id=${project_ids[index]}
  if wait "$pid"; then
      echo "$pid docker compose down successful"
  else
      echo "$pid docker compose down failed. Exiting."
      docker network inspect "${project_id}_default"
      exit 1  # Exit with an error code if any command fails
  fi
done

make clean build-race-docker build-race-fips-docker

export RUN_PARALLEL=${RUN_PARALLEL:-1}

docker compose -f "${CUR_DIR}/${COMPOSE_FILE}" --progress=quiet pull

pids=()
project_ids=()
for ((i = 0; i < RUN_PARALLEL; i++)); do
  docker compose -f "${CUR_DIR}/${COMPOSE_FILE}" --project-name project${i} --progress plain up -d &
  pids+=($!)
  project_ids+=("project${i}")
done

for index in "${!pids[@]}"; do
  pid=${pids[index]}
  project_id=${project_ids[index]}
  if wait "$pid"; then
      echo "$pid docker compose up successful"
  else
      docker compose -f "${CUR_DIR}/${COMPOSE_FILE}" --project-name "${project_id}" --progress plain logs
      echo "$pid the docker compose up failed."
      exit 1  # Exit with an error code if any command fails
  fi
done

set +e
go test -parallel "${RUN_PARALLEL}" -race -timeout "${TEST_TIMEOUT:-60m}" -failfast -tags=integration -run "${RUN_TESTS:-.+}" -v "${CUR_DIR}/integration_test.go"
TEST_FAILED=$?
set -e

if [[ "0" == "${TEST_FAILED}" ]]; then
  go tool covdata textfmt -i "${CUR_DIR}/_coverage_/" -o "${CUR_DIR}/_coverage_/coverage.out"
fi

if [[ "1" == "${CLEAN_AFTER:-0}" || "0" == "${TEST_FAILED}" ]]; then
  pids=()
  for project in $(docker compose -f "${CUR_DIR}/${COMPOSE_FILE}" ls --all -q); do
    docker compose -f "${CUR_DIR}/${COMPOSE_FILE}" --project-name "${project}" --progress plain down --remove-orphans --volumes --timeout=1 &
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

docker buildx prune -f --filter=until=30m --max-used-space=1G
