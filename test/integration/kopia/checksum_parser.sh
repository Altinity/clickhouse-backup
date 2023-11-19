#!/usr/bin/env bash
set -euo pipefail
command -v clickhouse-compressor || exit 0
CHECKSUM_FILE=$1
ACTION=$2
ACTION_DIR=$3
if [[ "ENC" == $(dd if="${CHECKSUM_FILE}" bs=1 skip="0" count="3" 2>/dev/null) ]]; then
  echo "ENCRYPTED FILES don't supported"
  exit 0
fi
FORMAT_VERSION=$(head -n +1 "${CHECKSUM_FILE}" | sed 's/checksums format version: //g')

log() { printf '%s\n' "$*"; }
error() { log "ERROR: $*" >&2; }
fatal() { error "$@"; exit 1; }

# appends a command to a trap
#
# - 1st arg:  code to add
# - remaining args:  names of traps to modify
#
trap_add() {
    trap_add_cmd=$1; shift || fatal "${FUNCNAME} usage error"
    for trap_add_name in "$@"; do
        trap -- "$(
            # helper fn to get existing trap command from output
            # of trap -p
            extract_trap_cmd() { printf '%s\n' "$3"; }
            # print existing trap command with newline
            eval "extract_trap_cmd $(trap -p "${trap_add_name}")"
            # print the new trap command
            printf '%s\n' "${trap_add_cmd}"
        )" "${trap_add_name}" \
            || fatal "unable to add to trap ${trap_add_name}"
    done
}

function checksums_body_cmd {
  if [[ "4" == "${FORMAT_VERSION}" ]]; then
    tail -n +2 "${CHECKSUM_FILE}" | clickhouse-compressor -d
  else
    tail -n +2 "${CHECKSUM_FILE}"
  fi
}


declare -g CURRENT_OFFSET=1
CURRENT_OFFSET_FIFO=$(mktemp -u)  # Generate a unique temporary file name
touch $CURRENT_OFFSET_FIFO
trap_add 'rm -f $CURRENT_OFFSET_FIFO' EXIT

function read_uvarint {
  readonly MaxVarintLen64=10
  readonly const0x80=$(printf "%d" 0x80)
  readonly const0x7f=$(printf "%d" 0x7f)
  local x=0
  local s=0

  for ((i=0; i<MaxVarintLen64; i++)); do
    read -r byte_value
    ((CURRENT_OFFSET += 1))
    echo $CURRENT_OFFSET > $CURRENT_OFFSET_FIFO
    if [ -z "$byte_value" ]; then
      if [ $i -gt 0 ]; then
        fatal "Error: unexpected end of file" >&2
      fi
      echo "$x"
      return
    fi

    if [ $byte_value -lt $const0x80 ]; then
      if [ $i -eq $((MaxVarintLen64-1)) ] && [ "$byte_value" -gt 1 ]; then
        fatal "Error: overflow" >&2
      fi
      x=$((x | (byte_value << s)))
      echo "$x"
      return
    fi

    x=$((x | ((byte_value & $const0x7f) << s)))
    s=$((s + 7))
  done

  echo "$x" >&2
  fatal "Error: overflow" >&2
}

TEMP_CHECKSUM_BODY=$(mktemp)
trap_add 'rm -f "${TEMP_CHECKSUM_BODY}"' EXIT

checksums_body_cmd > "${TEMP_CHECKSUM_BODY}"

ITEMS_COUNT=$(hexdump -v -e '/1 "%u\n"' "${TEMP_CHECKSUM_BODY}" | read_uvarint)
read CURRENT_OFFSET < $CURRENT_OFFSET_FIFO

for ((i=1; i<=$ITEMS_COUNT; i++)); do
  NAME_LENGTH=$(tail -c +$CURRENT_OFFSET "${TEMP_CHECKSUM_BODY}" | hexdump -v -e '/1 "%u\n"' | read_uvarint)
  read CURRENT_OFFSET < $CURRENT_OFFSET_FIFO

  NAME=$(dd if="${TEMP_CHECKSUM_BODY}" bs=1 skip="$((CURRENT_OFFSET-1))" count="${NAME_LENGTH}" 2>/dev/null)
  ((CURRENT_OFFSET += NAME_LENGTH))

  FILE_SIZE=$(tail -c +$CURRENT_OFFSET "${TEMP_CHECKSUM_BODY}" | hexdump -v -e '/1 "%u\n"' | read_uvarint)
  read CURRENT_OFFSET < $CURRENT_OFFSET_FIFO

  FILE_HASH=$(dd if="${TEMP_CHECKSUM_BODY}" bs=1 skip="$((CURRENT_OFFSET-1))" count="16" 2>/dev/null | xxd -ps -c 32)
  ((CURRENT_OFFSET += 16))

  IS_COMPRESSED=$(dd if="${TEMP_CHECKSUM_BODY}" bs=1 skip="$((CURRENT_OFFSET-1))" count="1" 2>/dev/null | xxd -p)
  ((CURRENT_OFFSET += 1))

  if [ "00" != "$IS_COMPRESSED" ]; then
    UNCOMPRESSED_SIZE=$(tail -c +$CURRENT_OFFSET "${TEMP_CHECKSUM_BODY}" | hexdump -v -e '/1 "%u\n"' | read_uvarint)
    read CURRENT_OFFSET < $CURRENT_OFFSET_FIFO

    UNCOMPRESSED_HASH=$(dd if="${TEMP_CHECKSUM_BODY}" bs=1 skip="$((CURRENT_OFFSET-1))" count="16" 2>/dev/null | xxd -ps -c 32)
    ((CURRENT_OFFSET += 16))
  fi
  if [[ "upload" == "$ACTION" ]]; then
    # echo "$(dirname ${CHECKSUM_FILE})/${NAME} -> ${ACTION_DIR}/${FILE_HASH}"
    cp -fl "$(dirname ${CHECKSUM_FILE})/${NAME}" "${ACTION_DIR}/${FILE_HASH}"
    rm "$(dirname ${CHECKSUM_FILE})/${NAME}"
  elif [[ "download" == "$ACTION" ]]; then
    # echo "${ACTION_DIR}/${FILE_HASH} -> $(dirname ${CHECKSUM_FILE})/${NAME}"
    cp -fl "${ACTION_DIR}/${FILE_HASH}" "$(dirname ${CHECKSUM_FILE})/${NAME}"
  else
    echo "${ACTION_DIR}/${FILE_HASH} <-> $(dirname ${CHECKSUM_FILE})/${NAME}"
  fi
done
