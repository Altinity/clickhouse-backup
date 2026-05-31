#!/usr/bin/env bash
# Verify a clickhouse-backup build is FIPS 140-3 compatible
# before the corresponding image is published to a registry (Docker Hub).
#
# This automates the following steps that may be done manually
# (after `docker pull altinity/clickhouse-backup:<tag>-fips`):
#   1. Verify the image contains `GODEBUG=fips140=only`
#      * docker run --rm --entrypoint sh <image> -c 'printenv | grep GODEBUG'
#      * check that GODEBUG contains `fips140=only`
#   2. Verify `clickhouse-backup --version` reports `FIPS 140-3: true`
#      * docker run --rm --entrypoint /bin/clickhouse-backup <image> --version
#      * grep "FIPS 140-3: true"
#   3. Verify the embedded Go binary was actually linked against the FIPS module
#      * go version -m <binary>
#      * check it shows GOFIPS140, the fips140 build tag, DefaultGODEBUG=fips140=on and CGO_ENABLED=0
#
# Usage:
#   verify_fips_image.sh --image <docker-image-ref> [--binary <path-to-fips-binary>]
#   verify_fips_image.sh --binary <path-to-fips-binary>
#
#   --image   A locally available (already built/loaded) FIPS docker image.
#             Runtime checks (env + --version) are performed against it.
#   --binary  A FIPS clickhouse-backup ELF binary. Build-metadata checks are
#             performed with `go version -m`. When omitted but --image is set,
#             the binary is copied out of the image and checked instead.
#
# Design note on the `go version -m` check:
#   We deliberately run `go version -m` with the host `go` toolchain against the
#   raw build artifact (or a binary copied out of the image). Mounting the host
#   `go` binary INTO the alpine FIPS image does not work reliably because the
#   image is musl-based while the host toolchain is glibc-linked, so it would
#   fail to execute. Running the check on the runner (where Go is already set
#   up) is simpler and more robust.

set -euo pipefail

IMAGE=""
BINARY=""

# Print the leading comment block. Using awk instead of hard-coded line numbers keeps the
# usage text correct even when the header above is edited.
usage() {
  awk 'NR==1 { next } /^#/ { print; next } { exit }' "$0" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)  IMAGE="${2:-}"; shift 2 ;;
    --binary) BINARY="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "${IMAGE}" && -z "${BINARY}" ]]; then
  echo "error: at least one of --image or --binary is required" >&2
  usage
  exit 1
fi

fail() { echo "FIPS VERIFY FAIL: $*" >&2; exit 1; }
ok()   { echo "FIPS VERIFY OK:   $*"; }

CLEANUP_BINARY=""
cleanup() {
  [[ -n "${CLEANUP_BINARY}" ]] && rm -f "${CLEANUP_BINARY}" || true
}
trap cleanup EXIT

verify_image_runtime() {
  local image="$1"
  local godebug_marker="GODEBUG=fips140=only"
  local version_label="FIPS 140-3:"

  echo "== image runtime checks: ${image} =="

  echo "-- check baked-in env contains ${godebug_marker}"
  local env_json
  if ! env_json="$(docker image inspect --format '{{json .Config.Env}}' "${image}" 2>&1)"; then
    fail "unable to inspect image ${image}: ${env_json}"
  fi
  grep -q "${godebug_marker}" <<<"${env_json}" \
    || fail "image ${image} does not set ${godebug_marker} (Env: ${env_json})"
  ok "image env enforces ${godebug_marker}"

  echo "-- check 'clickhouse-backup --version' reports ${version_label} true"
  local version_out
  if ! version_out="$(docker run --rm --entrypoint /bin/clickhouse-backup "${image}" --version 2>&1)"; then
    fail "unable to run 'clickhouse-backup --version' in ${image}: ${version_out}"
  fi
  printf '%s\n' "${version_out}"
  local fips_line
  fips_line="$(grep "${version_label}" <<<"${version_out}" || true)"
  [[ -n "${fips_line}" ]] || fail "'${version_label}' not present in --version output"
  grep -qi 'true' <<<"${fips_line}" \
    || fail "image ${image} reports non-FIPS build: ${fips_line}"
  ok "image reports ${version_label} true"
}

resolve_binary_from_image() {
  local image="$1"
  local tmp
  tmp="$(mktemp "${TMPDIR:-/tmp}/clickhouse-backup-fips.XXXXXX")"
  local cid
  cid="$(docker create "${image}")"
  docker cp "${cid}:/bin/clickhouse-backup" "${tmp}" >/dev/null
  docker rm -f "${cid}" >/dev/null
  CLEANUP_BINARY="${tmp}"
  echo "${tmp}"
}

verify_binary_metadata() {
  local binary="$1"
  # Markers emitted by `go version -m` for a binary linked against the Go FIPS module (GOFIPS140=...)
  local required_markers=(
    "GOFIPS140="
    "fips140"
    "DefaultGODEBUG=fips140=on"
    "CGO_ENABLED=0"
  )

  echo "== binary build-metadata checks: ${binary} =="
  [[ -f "${binary}" ]] || fail "binary not found: ${binary}"
  command -v go >/dev/null 2>&1 || fail "'go' toolchain not found in PATH; required for 'go version -m'"

  local meta
  if ! meta="$(go version -m "${binary}" 2>&1)"; then
    fail "'go version -m ${binary}' failed: ${meta}"
  fi

  local marker
  for marker in "${required_markers[@]}"; do
    if ! grep -q "${marker}" <<<"${meta}"; then
      printf '%s\n' "${meta}" >&2
      fail "binary ${binary} missing FIPS build marker: ${marker}"
    fi
    ok "binary build metadata contains: ${marker}"
  done
}

if [[ -n "${IMAGE}" ]]; then
  verify_image_runtime "${IMAGE}"
fi

BINARY_TO_CHECK="${BINARY}"
if [[ -z "${BINARY_TO_CHECK}" && -n "${IMAGE}" ]]; then
  echo "-- no --binary provided, copying /bin/clickhouse-backup out of ${IMAGE}"
  BINARY_TO_CHECK="$(resolve_binary_from_image "${IMAGE}")"
fi

if [[ -n "${BINARY_TO_CHECK}" ]]; then
  verify_binary_metadata "${BINARY_TO_CHECK}"
fi

echo ""
echo "FIPS 140-3 compatibility verification PASSED"
