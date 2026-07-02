#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop stream if exists test_tbl"
$CLICKHOUSE_CLIENT -q "create stream test_tbl (a string, b string, c string) order by a"
cat $CURDIR/data_csv/csv_with_cr_end_of_line.csv | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_tbl(a, b, c) SETTINGS input_format_csv_allow_cr_end_of_line=true FORMAT CSV"
$CLICKHOUSE_CLIENT -q "select sleep(2) format Null"
$CLICKHOUSE_CLIENT -q "select a, b, c from table(test_tbl)"
$CLICKHOUSE_CLIENT -q "drop stream test_tbl"