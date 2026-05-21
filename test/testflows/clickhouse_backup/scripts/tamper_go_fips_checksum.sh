#!/usr/bin/env bash
# Flip one byte in go:fipsinfo embedded Sum so the Go FIPS module integrity check fails at init.
#
# bash scripts/tamper_go_fips_checksum.sh [PATH_TO_ELF]
#
# PATH defaults to CLICKHOUSE_BACKUP_FIPS_BINARY when unset.
# Requires: readelf (binutils), python3
#
# Exits:
#   0 tamper reproduced (verification mismatch in output)
#   1 usage / missing tool / bad args
#   2 not a GOFIPS140 binary (.go.fipsinfo missing)
#   3 could not parse section offset from readelf
#   4 tampered run did not print expected panic text

set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Flip one byte in go:fipsinfo to force "fips140: verification mismatch".

Usage:
  tamper_go_fips_checksum.sh [-k] [BINARY]

  -k        Keep tampered temp file and print its path
  BINARY    defaults to CLICKHOUSE_BACKUP_FIPS_BINARY

Requires: readelf, python3
EOF
}

KEEP=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    -k|--keep) KEEP=1; shift ;;
    *) break ;;
  esac
done

BIN="${1:-${CLICKHOUSE_BACKUP_FIPS_BINARY:-}}"
if [[ -z "${BIN}" ]]; then
  echo "error: pass ELF path or set CLICKHOUSE_BACKUP_FIPS_BINARY" >&2
  exit 1
fi
if [[ ! -f "${BIN}" ]]; then
  echo "error: not a file: ${BIN}" >&2
  exit 1
fi

for cmd in readelf python3; do
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "error: '${cmd}' not found in PATH" >&2
    exit 1
  fi
done

echo "== check: FIPS section .go.fipsinfo (GOFIPS140 Go crypto module marker) =="
RELF_OUT=$(readelf -S -W "${BIN}")
if ! grep -q '\.go\.fipsinfo' <<<"${RELF_OUT}"; then
  echo "error: no '.go.fipsinfo' section - build with GOFIPS140 (e.g. GOFIPS140=v1.0.0) or this is not the FIPS-linked artifact." >&2
  exit 2
fi

FILE_OFF_HEX=$(
  python3 -c '
import sys
text = sys.stdin.read().splitlines()
for line in text:
    if ".go.fipsinfo" not in line:
        continue
    parts = line.split()
    if len(parts) < 6:
        continue
    for i, w in enumerate(parts):
        if w == ".go.fipsinfo" and i + 3 < len(parts):
            # name PROGBITS vma off size ...
            print(parts[i + 3])
            sys.exit(0)
sys.exit(1)
' <<< "${RELF_OUT}"
) || true

if [[ -z "${FILE_OFF_HEX}" ]]; then
  echo "error: could not parse .go.fipsinfo file offset from readelf -S -W output" >&2
  exit 3
fi

# Section: 16-byte magic, then 32-byte HMAC (crypto/internal/fips140/check.Linkinfo)
SEC_OFF=$((16#${FILE_OFF_HEX}))
HMAC_OFF=$((SEC_OFF + 16))

echo "  .go.fipsinfo file offset (hex): ${FILE_OFF_HEX} (decimal ${SEC_OFF})"
echo "  first byte of Sum to XOR: ${HMAC_OFF}"

WORK=$(mktemp "${TMPDIR:-/tmp}/go-fips-tamper.XXXXXX")
cp "${BIN}" "${WORK}"
chmod +x "${WORK}"

cleanup() {
  if [[ -n "${KEEP}" ]]; then
    echo "kept tampered ELF: ${WORK}"
  else
    rm -f "${WORK}"
  fi
}
trap cleanup EXIT

echo "== XOR one byte in a temp copy of the ELF =="
python3 -c "
import pathlib, sys
p = pathlib.Path(sys.argv[1])
n = int(sys.argv[2])
b = bytearray(p.read_bytes())
b[n] ^= 0xFF
p.write_bytes(b)
" "${WORK}" "${HMAC_OFF}"

echo "== run tampered binary (expect panic text fips140: verification mismatch) =="
set +e
OUT=$(GODEBUG=fips140=on "${WORK}" --version 2>&1)
RC=$?
set -e
printf '%s\n' "${OUT}"
echo "exit code: ${RC}"

OK_MSG=0
if printf '%s\n' "${OUT}" | grep -q 'fips140: verification mismatch'; then
  OK_MSG=1
fi

if [[ "${OK_MSG}" -eq 1 ]]; then
  echo "== OK: FIPS integrity check failed as expected =="
  exit 0
fi

echo "error: did not find 'fips140: verification mismatch' in output" >&2
if [[ "${RC}" -eq 0 ]]; then
  echo "hint: tampered binary exited 0; init self-test may not run on --version for this build" >&2
fi
exit 4
