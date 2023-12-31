#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1"

${CLICKHOUSE_CLIENT} -q "DROP STREAM IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} -q "create stream async_inserts (id uint32, s string) ENGINE = MergeTree ORDER BY id"

${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO async_inserts FORMAT CustomSeparated settings format_custom_escaping_rule='CSV', format_custom_field_delimiter=','
1,\"a\"
2,\"b\"
" &

${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO async_inserts FORMAT CustomSeparated settings format_custom_escaping_rule='CSV', format_custom_field_delimiter=','
3,\"a\"
4,\"b\"
" &

${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO async_inserts FORMAT CustomSeparatedWithNames settings format_custom_escaping_rule='CSV', format_custom_field_delimiter=','
\"id\",\"s\"
5,\"a\"
6,\"b\"
" &

${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO async_inserts FORMAT CustomSeparatedWithNames settings format_custom_escaping_rule='CSV', format_custom_field_delimiter=','
\"id\",\"s\"
7,\"a\"
8,\"b\"
" &

wait

${CLICKHOUSE_CLIENT} -q "SELECT * FROM async_inserts ORDER BY id"
${CLICKHOUSE_CLIENT} -q "SELECT name, rows, level FROM system.parts WHERE table = 'async_inserts' AND database = '$CLICKHOUSE_DATABASE' ORDER BY name"

${CLICKHOUSE_CLIENT} -q "DROP STREAM async_inserts"
