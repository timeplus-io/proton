#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1"

${CLICKHOUSE_CLIENT} -q "DROP STREAM IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} -q "create stream async_inserts (id uint32, s string) ENGINE = MergeTree ORDER BY id SETTINGS parts_to_throw_insert = 1"
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES $CLICKHOUSE_DATABASE.async_inserts"

${CLICKHOUSE_CLIENT} -q "INSERT INTO async_inserts VALUES (1, 's')"

for _ in {1..3}; do
    ${CLICKHOUSE_CURL} -sS $url -d 'INSERT INTO async_inserts FORMAT JSONEachRow {"id": 2, "s": "a"} {"id": 3, "s": "b"}' \
        | grep -o "Too many parts" &
done

wait

${CLICKHOUSE_CLIENT} -q "DROP STREAM async_inserts"
