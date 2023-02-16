#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE STREAM floats (v float64) Engine=MergeTree() ORDER BY tuple();"
$CLICKHOUSE_CLIENT -q "CREATE STREAM target_1 (v float64) Engine=MergeTree() ORDER BY tuple();"
$CLICKHOUSE_CLIENT -q "CREATE STREAM target_2 (v float64) Engine=MergeTree() ORDER BY tuple();"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW floats_to_target TO target_1 AS SELECT * FROM floats"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW floats_to_target_2 TO target_2 AS SELECT * FROM floats, numbers(2) n"

echo "No materialized views"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&wait_end_of_query=1&query=INSERT+INTO+target_1" -d "VALUES(1.0)" -v 2>&1 | grep 'X-ClickHouse-Summary'
$CLICKHOUSE_LOCAL -q "SELECT number::float64 AS v FROM numbers(10)" --format Native | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&wait_end_of_query=1&query=INSERT+INTO+target_1+FORMAT+Native" --data-binary @- -v 2>&1 | grep 'X-ClickHouse-Summary'
$CLICKHOUSE_LOCAL -q "SELECT number::float64 AS v FROM numbers(10)" --format RowBinary | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&wait_end_of_query=1&query=INSERT+INTO+target_1+FORMAT+RowBinary" --data-binary @- -v 2>&1 | grep 'X-ClickHouse-Summary'

echo "With materialized views"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&wait_end_of_query=1&query=INSERT+INTO+floats" -d "VALUES(1.0)" -v 2>&1 | grep 'X-ClickHouse-Summary'
$CLICKHOUSE_LOCAL -q "SELECT number::float64 AS v FROM numbers(10)" --format Native | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&wait_end_of_query=1&query=INSERT+INTO+floats+FORMAT+Native" --data-binary @- -v 2>&1 | grep 'X-ClickHouse-Summary'
$CLICKHOUSE_LOCAL -q "SELECT number::float64 AS v FROM numbers(10)" --format RowBinary | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&wait_end_of_query=1&query=INSERT+INTO+floats+FORMAT+RowBinary" --data-binary @- -v 2>&1 | grep 'X-ClickHouse-Summary'
