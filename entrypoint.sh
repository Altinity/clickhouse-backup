#!/bin/bash
if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]] || [[ ! -x  $(command -v "$1") ]]; then
    exec /bin/clickhouse-backup "$@"
fi
exec "$@"
