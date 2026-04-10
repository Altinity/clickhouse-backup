#!/bin/bash
set -x
set -e

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
export CUR_DIR
mkdir -p "${CUR_DIR}/_coverage_/"
rm -rf "${CUR_DIR}/_coverage_/*"

source "${CUR_DIR}/.env"

export CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION:-26.3}
if [[ "${CLICKHOUSE_VERSION}" =~ ^2[1-9]+ || "${CLICKHOUSE_VERSION}" == "head" ]]; then
  export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server}
else
  export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-yandex/clickhouse-server}
fi
CLICKHOUSE_BACKUP_BIN="$(pwd)/clickhouse-backup/clickhouse-backup-race"
export CLICKHOUSE_BACKUP_BIN
export LOG_LEVEL=${LOG_LEVEL:-info}
export TEST_LOG_LEVEL=${TEST_LOG_LEVEL:-info}

GCS_ENCRYPTION_KEY=$(openssl rand -base64 32)
export GCS_ENCRYPTION_KEY

if [[ -f "${CUR_DIR}/credentials.json" ]]; then
  export GCS_TESTS=${GCS_TESTS:-1}
else
  export GCS_TESTS=${GCS_TESTS:-}
fi

export GLACIER_TESTS=${GLACIER_TESTS:-0}

export AZURE_TESTS=${AZURE_TESTS:-1}
export RUN_ADVANCED_TESTS=${RUN_ADVANCED_TESTS:-1}
export KEEPER_TLS_ENABLED=${KEEPER_TLS_ENABLED:-1}
export GODEBUG=${GODEBUG:-}
export S3_DEBUG=${S3_DEBUG:-false}
export GCS_DEBUG=${GCS_DEBUG:-false}
export FTP_DEBUG=${FTP_DEBUG:-false}
export SFTP_DEBUG=${SFTP_DEBUG:-false}
export AZBLOB_DEBUG=${AZBLOB_DEBUG:-false}
export COS_DEBUG=${COS_DEBUG:-false}
export CLICKHOUSE_DEBUG=${CLICKHOUSE_DEBUG:-false}

export RUN_PARALLEL=${RUN_PARALLEL:-1}

make clean build-race-docker build-race-fips-docker

set +e
go test -count=1 -parallel "${RUN_PARALLEL}" -race -timeout "${TEST_TIMEOUT:-60m}" -failfast -tags=integration -shuffle="${SHUFFLE:-$(date +%Y%m%d)}" -run "${RUN_TESTS:-.+}" -v "${CUR_DIR}/..."
TEST_FAILED=$?
set -e

if [[ "0" == "${TEST_FAILED}" ]]; then
  go tool covdata textfmt -i "${CUR_DIR}/_coverage_/" -o "${CUR_DIR}/_coverage_/coverage.out"
fi

docker buildx prune -f --filter=until=30m --max-used-space=1G

exit ${TEST_FAILED}