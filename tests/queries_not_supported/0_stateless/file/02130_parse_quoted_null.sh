#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
DATA_FILE=$USER_FILES_PATH/test_02130.data
SELECT_QUERY="select * from file('test_02130.data', 'CustomSeparated', 'x nullable(float64), y nullable(uint64)') settings input_format_parallel_parsing=0, format_custom_escaping_rule='Quoted'"


$CLICKHOUSE_CLIENT -q "drop stream if exists test_02130"
$CLICKHOUSE_CLIENT -q "create stream test_02130 (x nullable(float64), y nullable(uint64)) engine=Memory()"

echo -e "null\t1" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY"

echo -e "nan\t2" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY"

echo -e "42.42\t3" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY"

echo -e "null\t4" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=1 --storage_file_read_method=pread

echo -e "null\t5" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=2 --storage_file_read_method=pread

echo -e "null\t6" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=3 --storage_file_read_method=pread

echo -e "null\t7" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=4 --storage_file_read_method=pread

echo -e "nan\t8" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=1 --storage_file_read_method=pread

echo -e "nan\t9" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=2 --storage_file_read_method=pread

echo -e "nan\t10" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=3 --storage_file_read_method=pread

echo -e "nan\t11" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=4 --storage_file_read_method=pread

echo -e "42\tnan" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "$SELECT_QUERY" --max_read_buffer_size=4 --storage_file_read_method=pread 2>&1 | grep -F -q "CANNOT_READ_ALL_DATA" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -q "select * from test_02130 order by y"
$CLICKHOUSE_CLIENT -q "drop stream test_02130"

rm $DATA_FILE
