#!/bin/bash
set -o pipefail
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

DEBUG_FLAG=""
if [[ -n "${TESTFLOWS_DEBUG}" ]]; then
  DEBUG_FLAG="--debug"
fi

RUN_PARALLEL=${RUN_PARALLEL:-1}
REGRESSION_PY="${CUR_DIR}/clickhouse_backup/regression.py"

# Auto-discover suite names from regression.py
discover_suites() {
  python3 -c "
import re, sys
with open(sys.argv[1]) as f:
    text = f.read()
for m in re.finditer(r'Scenario\(run=load\(\"clickhouse_backup\.tests\.\w+\",\s*\"(\w+)\"', text):
    print(m.group(1).replace('_', ' '))
" "${REGRESSION_PY}"
}

if [[ -n "${RUN_TESTS}" ]]; then
  # Single suite mode — run exactly what was requested
  python3 "${REGRESSION_PY}" ${DEBUG_FLAG} --no-colors --only="${RUN_TESTS}"
else
  SUITES=()
  while IFS= read -r line; do
    SUITES+=("$line")
  done < <(discover_suites)
  if [[ ${#SUITES[@]} -eq 0 ]]; then
    echo "ERROR: no suites discovered from ${REGRESSION_PY}"
    exit 1
  fi

  mkdir -p "${CUR_DIR}/_logs_"
  echo "Discovered ${#SUITES[@]} suites: ${SUITES[*]}"
  echo "Running with parallelism=${RUN_PARALLEL}"

  printf '%s\n' "${SUITES[@]}" | xargs -P "${RUN_PARALLEL}" -I{} bash -c '
    suite="$1"
    log_file="'"${CUR_DIR}"'/_logs_/${suite// /_}.log"
    echo "=== Starting suite: ${suite} ==="
    python3 "'"${REGRESSION_PY}"'" \
      '"${DEBUG_FLAG}"' --no-colors \
      --only="/clickhouse backup/${suite}*" \
      > "${log_file}" 2>&1
    rc=$?
    if [[ ${rc} -ne 0 ]]; then
      echo "=== FAIL: ${suite} (exit code ${rc}), see ${log_file} ==="
    else
      echo "=== PASS: ${suite} ==="
    fi
    exit ${rc}
  ' _ {}

  echo ""
  echo "=== Results ==="
  FAIL_COUNT=0
  for suite in "${SUITES[@]}"; do
    log_file="${CUR_DIR}/_logs_/${suite// /_}.log"
    if grep -q "Failing" "${log_file}" 2>/dev/null; then
      echo "  FAIL: ${suite}"
      FAIL_COUNT=$((FAIL_COUNT + 1))
    else
      echo "  PASS: ${suite}"
    fi
  done
  echo "=== Failures: ${FAIL_COUNT}/${#SUITES[@]} ==="
fi

go tool covdata textfmt -i "${CUR_DIR}/_coverage_/" -o "${CUR_DIR}/_coverage_/coverage.out"
docker buildx prune -f --filter=until=30m --max-used-space=1G
