#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(${CLICKHOUSE_CLIENT} --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 string')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_03713_payload.bin"
DATA_FILE="${USER_FILES_PATH}/${FILE_NAME}"

$CLICKHOUSE_CLIENT -q "insert into table function file('${FILE_NAME}', 'RawBLOB', 's string') select unhex('010178') || repeat(unhex('1e'), 1000000) settings engine_file_truncate_on_insert=1"

$CLICKHOUSE_CLIENT -q "desc file('${FILE_NAME}', 'RowBinaryWithNamesAndTypes') settings input_format_binary_decode_types_in_binary_format=1" 2>&1 | grep -F -q "CANNOT_EXTRACT_STREAM_STRUCTURE" && echo "OK" || echo "FAIL"

rm -f "${DATA_FILE}"
