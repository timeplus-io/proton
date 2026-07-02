#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_FILES_PATH=$(${CLICKHOUSE_CLIENT} --query "select _path, _file from file('nonexist.txt', 'CSV', 'val1 string')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

$CLICKHOUSE_CLIENT -q "select 'Hello\rWorld' from numbers(1000000) format TSVRaw" > $USER_FILES_PATH/02905_csv_unquoted_string_with_cr.csv
$CLICKHOUSE_CLIENT -q "desc file('02905_csv_unquoted_string_with_cr.csv')"
$CLICKHOUSE_CLIENT -q "select count() from file('02905_csv_unquoted_string_with_cr.csv') settings optimize_count_from_files=0"
$CLICKHOUSE_CLIENT -q "select count() from file('02905_csv_unquoted_string_with_cr.csv') settings optimize_count_from_files=1"
$CLICKHOUSE_CLIENT -q "select * from file('02905_csv_unquoted_string_with_cr.csv') limit 1"

rm $USER_FILES_PATH/02905_csv_unquoted_string_with_cr.csv

