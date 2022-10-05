#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

exception_pattern="Code: 48.*Method write is not supported by storage View"

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS test_00595;"
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS test_view_00595;"

${CLICKHOUSE_CLIENT} --query "create stream test_00595 (s string)  ;"
${CLICKHOUSE_CLIENT} --query "CREATE VIEW test_view_00595 AS SELECT * FROM test_00595;"

(( $(${CLICKHOUSE_CLIENT} --query "INSERT INTO test_view_00595(s) VALUES('test_string');" 2>&1 | grep -c "$exception_pattern") >= 1 )) && echo 1 || echo "NO MATCH"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_00595(s) VALUES('test_string');"
sleep 3

${CLICKHOUSE_CLIENT} --query "SELECT * FROM table(test_00595) settings asterisk_include_reserved_columns=false;"

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS test_00595;"
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS test_view_00595;"
