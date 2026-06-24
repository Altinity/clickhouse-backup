#!/usr/bin/env bash
# to avoid backward incompatibility ;(
# https://t.me/clickhouse_ru/359960
# https://t.me/clickhouse_ru/359968
# https://t.me/clickhouse_ru/362378

if [ $# -ne 0 ]; then
    /entrypoint.sh "$@"
else
    /docker-entrypoint-initdb.d/dynamic_settings.sh
    /entrypoint.sh
fi
