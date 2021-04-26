#!/bin/bash
if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]] || [[ ! -f "$1" ]]; then
    exec /bin/clickhouse-backup "$@"
fi
exec "$@"
