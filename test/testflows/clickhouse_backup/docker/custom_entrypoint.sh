#!/usr/bin/env bash
# to avoid backward incompatibility ;(
# https://t.me/clickhouse_ru/359960
# https://t.me/clickhouse_ru/359968
# https://t.me/clickhouse_ru/362378

if [ $# -ne 0 ]; then
    /entrypoint.sh "$@"
else
    for script in /docker-entrypoint-initdb.d/*.sh; do
      $script
    done
    /entrypoint.sh
fi
