#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1"

${CLICKHOUSE_CLIENT} -q "DROP STREAM IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} -q "create stream async_inserts (id uint32, s string) "

${CLICKHOUSE_CURL} -sS $url -d "INSERT INTO async_inserts VALUES (1, 'a') (2, 'b')" &
${CLICKHOUSE_CURL} -sS $url -d "INSERT INTO async_inserts VALUES (3, 'c'), (4, 'd')" &
${CLICKHOUSE_CURL} -sS $url -d "INSERT INTO async_inserts VALUES (5, 'e'), (6, 'f'), " &

wait

${CLICKHOUSE_CLIENT} -q "SELECT * FROM async_inserts ORDER BY id"

${CLICKHOUSE_CLIENT} -q "DROP STREAM async_inserts"
