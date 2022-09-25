#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME=test_02149.data
DATA_FILE=$USER_FILES_PATH/$FILE_NAME

for format in Arrow ArrowStream Parquet ORC Native TSVWithNamesAndTypes TSVRawWithNamesAndTypes CSVWithNamesAndTypes JSONCompactEachRowWithNamesAndTypes JSONCompactStringsEachRowWithNamesAndTypes RowBinaryWithNamesAndTypes CustomSeparatedWithNamesAndTypes
do
    echo $format
    $CLICKHOUSE_CLIENT -q "select to_int8(-number) as int8, to_uint8(number) as uint8, to_int16(-number) as int16, to_uint16(number) as uint16, to_int32(-number) as int32, to_uint32(number) as uint32, to_int64(-number) as int64, to_uint64(number) as uint64 from numbers(2) format $format" > $DATA_FILE
    $CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', '$format')"
    $CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', '$format')"

    $CLICKHOUSE_CLIENT -q "select to_float32(number * 1.2) as float32, to_float64(number / 1.3) as float64, to_decimal32(number / 0.3, 5) as decimal32, to_decimal64(number / 0.003, 5) as decimal64 from numbers(2) format $format" > $DATA_FILE
    $CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', '$format')"
    $CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', '$format')"

    $CLICKHOUSE_CLIENT -q "select to_date(number) as date, toDate32(number) as date32 from numbers(2) format $format" > $DATA_FILE
    $CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', '$format')"
    $CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', '$format')"

    $CLICKHOUSE_CLIENT -q "select concat('Str: ', to_string(number)) as str, to_fixed_string(to_string((number + 1) * 100 % 1000), 3) as fixed_string from numbers(2) format $format" > $DATA_FILE
    $CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', '$format')"
    $CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', '$format')"
 
    $CLICKHOUSE_CLIENT -q "select [number, number + 1] as array, (number, to_string(number)) as tuple, map(to_string(number), number) as map from numbers(2) format $format" > $DATA_FILE
    $CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', '$format')"
    $CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', '$format')"

    $CLICKHOUSE_CLIENT -q "select [([number, number + 1], map('42', number)), ([], map()), ([42], map('42', 42))] as nested1, (([[number], [number + 1], []], map(number, [(number, '42'), (number + 1, '42')])), 42) as nested2 from numbers(2) format $format" > $DATA_FILE
    $CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', '$format')"
    $CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', '$format')"
done

echo "Avro"

echo $format
$CLICKHOUSE_CLIENT -q "select to_int8(-number) as int8, to_uint8(number) as uint8, to_int16(-number) as int16, to_uint16(number) as uint16, to_int32(-number) as int32, to_uint32(number) as uint32, to_int64(-number) as int64, to_uint64(number) as uint64 from numbers(2) format Avro" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Avro')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Avro')"

$CLICKHOUSE_CLIENT -q "select to_float32(number * 1.2) as float32, to_float64(number / 1.3) as float64 from numbers(2) format Avro" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Avro')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Avro')"

$CLICKHOUSE_CLIENT -q "select to_date(number) as date from numbers(2) format Avro" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Avro')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Avro')"

$CLICKHOUSE_CLIENT -q "select concat('Str: ', to_string(number)) as str, to_fixed_string(to_string((number + 1) * 100 % 1000), 3) as fixed_string from numbers(2) format Avro" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Avro')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Avro')"
 
$CLICKHOUSE_CLIENT -q "select [number, number + 1] as array, [[[number], [number + 1]]] as nested from numbers(2) format Avro" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Avro')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Avro')"

rm $DATA_FILE

