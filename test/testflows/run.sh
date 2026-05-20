#!/bin/bash
set -o pipefail
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
mkdir -p "${CUR_DIR}/_coverage_/"
rm -rf "${CUR_DIR}/_coverage_/*"
if [[ -f "${CUR_DIR}/.env" ]]; then
  source "${CUR_DIR}/.env"
fi
export CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION:-26.3}
if [[ "${CLICKHOUSE_VERSION}" =~ ^2[1-9]+ || "${CLICKHOUSE_VERSION}" == "head" ]]; then
  export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server}
else
  export CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-yandex/clickhouse-server}
fi
if [[ -z "${GITHUB_ACTIONS}" ]]; then
  make clean build-race-docker
fi

# Flags passed through to regression.py
EXTRA_FLAGS=""
if [[ -n "${TESTFLOWS_DEBUG}" || -n "${DEBUG}" ]]; then
  EXTRA_FLAGS="${EXTRA_FLAGS} --debug"
fi
if [[ "${NO_COLORS}" == "1" ]]; then
  EXTRA_FLAGS="${EXTRA_FLAGS} --no-colors"
fi

RAW_LOG="${LOG_FILE:-${CUR_DIR}/raw.log}"

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

TEST_FAILED=0
START_TIME=${SECONDS}
if [[ -n "${RUN_TESTS}" && "${RUN_TESTS}" != "*" ]]; then
  # Single suite mode — run exactly what was requested
  python3 "${REGRESSION_PY}" ${EXTRA_FLAGS} --only="${RUN_TESTS}" --log "${RAW_LOG}" || TEST_FAILED=1
  ELAPSED=$(( SECONDS - START_TIME ))
  echo "=== Total time: $(( ELAPSED / 60 ))m$(( ELAPSED % 60 ))s ==="
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
  rm -f "${CUR_DIR}/_logs_"/*.rc
  echo "Discovered ${#SUITES[@]} suites: ${SUITES[*]}"
  echo "Running with parallelism=${RUN_PARALLEL}"

  printf '%s\n' "${SUITES[@]}" | xargs -P "${RUN_PARALLEL}" -I{} bash -c '
    suite="$1"
    suite_slug="${suite// /_}"
    log_file="'"${CUR_DIR}"'/_logs_/${suite_slug}.log"
    rc_file="'"${CUR_DIR}"'/_logs_/${suite_slug}.rc"
    echo "=== Starting suite: ${suite} ==="
    suite_start=${SECONDS}
    python3 "'"${REGRESSION_PY}"'" \
      '"${EXTRA_FLAGS}"' \
      --only="/clickhouse backup/${suite}*" \
      --log "${log_file}" \
      > "${log_file}.stdout" 2>&1
    rc=$?
    echo "${rc}" > "${rc_file}"
    suite_elapsed=$(( SECONDS - suite_start ))
    suite_min=$(( suite_elapsed / 60 ))
    suite_sec=$(( suite_elapsed % 60 ))
    if [[ ${rc} -ne 0 ]]; then
      echo "=== FAIL: ${suite} (${suite_min}m${suite_sec}s, exit code ${rc}) ==="
      echo "=== stdout ==="
      cat "${log_file}.stdout"
      echo "=== end stdout ==="
    else
      echo "=== PASS: ${suite} (${suite_min}m${suite_sec}s) ==="
    fi
    exit ${rc}
  ' _ {}

  # Merge per-suite logs into single raw log
  cat "${CUR_DIR}"/_logs_/*.log > "${RAW_LOG}" 2>/dev/null || true

  echo ""
  echo "=== Results ==="
  FAIL_COUNT=0
  for suite in "${SUITES[@]}"; do
    suite_slug="${suite// /_}"
    log_file="${CUR_DIR}/_logs_/${suite_slug}.log"
    rc_file="${CUR_DIR}/_logs_/${suite_slug}.rc"
    suite_rc=0
    if [[ -f "${rc_file}" ]]; then
      suite_rc=$(cat "${rc_file}")
    else
      # rc file missing means suite never ran or was killed
      suite_rc=1
    fi
    if [[ "${suite_rc}" -ne 0 ]]; then
      echo "  FAIL: ${suite} (exit code ${suite_rc})"
      FAIL_COUNT=$((FAIL_COUNT + 1))
    else
      echo "  PASS: ${suite}"
    fi
  done
  ELAPSED=$(( SECONDS - START_TIME ))
  echo "=== Failures: ${FAIL_COUNT}/${#SUITES[@]}, total time: $(( ELAPSED / 60 ))m$(( ELAPSED % 60 ))s ==="
  if [[ "${FAIL_COUNT}" -gt 0 ]]; then
    TEST_FAILED=1
  fi
fi

# Generate tfs reports if tfs is available and raw log exists
if command -v tfs &>/dev/null && [[ -f "${RAW_LOG}" ]]; then
  TFS_FLAGS=""
  if [[ -n "${TESTFLOWS_DEBUG}" || -n "${DEBUG}" ]]; then
    TFS_FLAGS="${TFS_FLAGS} --debug"
  fi
  if [[ "${NO_COLORS}" == "1" ]]; then
    TFS_FLAGS="${TFS_FLAGS} --no-colors"
  fi
  tfs ${TFS_FLAGS} transform compact "${RAW_LOG}" "${CUR_DIR}/compact.log" || true
  tfs ${TFS_FLAGS} transform nice "${RAW_LOG}" "${CUR_DIR}/nice.log.txt" || true
  tfs ${TFS_FLAGS} transform short "${RAW_LOG}" "${CUR_DIR}/short.log.txt" || true
  if [[ -n "${GITHUB_SERVER_URL}" && -n "${GITHUB_REPOSITORY}" && -n "${GITHUB_RUN_ID}" ]]; then
    tfs ${TFS_FLAGS} report results \
      -a "${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/" \
      "${RAW_LOG}" - \
      --confidential --copyright "Altinity LTD" --logo "${CUR_DIR}/altinity.png" \
      | tfs ${TFS_FLAGS} document convert > "${CUR_DIR}/report.html" || true
  fi
fi

# Fix permissions for CI artifact upload
if [[ -d "${CUR_DIR}/clickhouse_backup/_instances" ]]; then
  sudo chmod -Rv +rx "${CUR_DIR}/clickhouse_backup/_instances" || true
fi

go tool covdata textfmt -i "${CUR_DIR}/_coverage_/" -o "${CUR_DIR}/_coverage_/coverage.out" || true
docker buildx prune -f --filter=until=30m --max-used-space=1G || true

exit ${TEST_FAILED}
