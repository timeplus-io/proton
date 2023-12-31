#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0"

${CLICKHOUSE_CLIENT} -q "DROP STREAM IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} -q "create stream async_inserts (id uint32, s string) "

${CLICKHOUSE_CURL} -sS $url -d 'INSERT INTO async_inserts1 FORMAT JSONEachRow {"id": 1, "s": "a"}'  \
    | grep -o "Code: 60"

${CLICKHOUSE_CURL} -sS $url -d 'INSERT INTO async_inserts FORMAT BadFormat {"id": 1, "s": "a"}'  \
    | grep -o "Code: 73"

${CLICKHOUSE_CURL} -sS $url -d 'INSERT INTO async_inserts FORMAT Pretty {"id": 1, "s": "a"}'  \
    | grep -o "Code: 73"

${CLICKHOUSE_CURL} -sS $url -d 'INSERT INTO async_inserts (id, a) FORMAT JSONEachRow {"id": 1, "s": "a"}'  \
    | grep -o "Code: 16"

${CLICKHOUSE_CLIENT} -q "DROP STREAM async_inserts"
