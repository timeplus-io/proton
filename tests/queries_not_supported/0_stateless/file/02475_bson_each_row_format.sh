#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "integers"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select number::bool as bool, number::int8 as int8, number::uint8 as uint8, number::int16 as int16, number::uint16 as uint16, number::int32 as int32, number::uint32 as uint32, number::int64 as int64, number::uint64 as uint64 from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'bool bool, int8 int8, uint8 uint8, int16 int16, uint16 uint16, int32 int32, uint32 uint32, int64 int64, uint64 uint64')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"

echo "integers conversion"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'uint64 uint64, int64 int64') select 4294967297, -4294967297 settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uint64 uint32, int64 uint32')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uint64 int32, int64 int32')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uint64 uint16, int64 uint16')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uint64 int16, int64 int16')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uint64 uint8, int64 uint8')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uint64 int8, int64 int8')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "floats"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'float32 float32, float64 float64') select number / (number + 1), number / (number + 1) from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'float32 float32, float64 float64')";


$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "Big integers"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'int128 int128, uint128 uint128, int256 int256, uint256 uint256') select number * -10000000000000000000000::int128 as int128, number * 10000000000000000000000::uint128 as uint128, number * -100000000000000000000000000000000000000000000::int256 as int256, number * 100000000000000000000000000000000000000000000::uint256 as uint256 from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'int128 int128, uint128 uint128, int256 int256, uint256 uint256')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"


echo "Dates"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'date Date, date32 Date32, datetime DateTime(\'UTC\'), datetime64 DateTime64(6, \'UTC\')') select number, number, number, number from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'date Date, date32 Date32, datetime DateTime(\'UTC\'), datetime64 DateTime64(6, \'UTC\')')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "decimals"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'decimal32 decimal32(3), decimal64 decimal64(6), decimal128 decimal128(12), decimal256 decimal256(24)') select number * 42.422::decimal32(3) as decimal32, number * 42.424242::decimal64(6) as decimal64, number * 42.424242424242::decimal128(12) as decimal128, number * 42.424242424242424242424242::decimal256(24) as decimal256 from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'decimal32 decimal32(3), decimal64 decimal64(6), decimal128 decimal128(12), decimal256 decimal256(24)')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"


echo "strings"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'str string, fixstr fixed_string(5)') select repeat('HelloWorld', number), repeat(char(97 + number), number % 6) from numbers(5) settings engine_file_truncate_on_insert=1, output_format_bson_string_as_string=0"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'str string, fixstr fixed_string(5)')"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'str string, fixstr fixed_string(5)') select repeat('HelloWorld', number), repeat(char(97 + number), number % 6) from numbers(5) settings engine_file_truncate_on_insert=1, output_format_bson_string_as_string=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'str string, fixstr fixed_string(5)')"


$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "uuid"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'uuid uuid') select 'b86d5c23-4b87-4465-8f33-4a685fa1c868'::uuid settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'uuid uuid')"


$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "low_cardinality"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'lc low_cardinality(string)') select char(97 + number % 3)::low_cardinality(string) from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'lc low_cardinality(string)')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "nullable"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'null nullable(uint32)') select number % 2 ? NULL : number from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'null nullable(uint32)')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'null uint32')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'null uint32') settings input_format_null_as_default=0" 2>&1 | grep -q -F "INCORRECT_DATA" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "low_cardinality(nullable)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'lc low_cardinality(nullable(string))') select number % 2 ? NULL : char(97 + number % 3)::low_cardinality(string) from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'lc low_cardinality(nullable(string))')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "array"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'arr1 array(uint64), arr2 array(string)') select range(number), ['Hello'] from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'arr1 array(uint64), arr2 array(string)') settings engine_file_truncate_on_insert=1" 

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "tuple"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'tuple tuple(x uint64, s string)') select tuple(number, 'Hello') from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple tuple(x uint64, s string)')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple tuple(s string, x uint64)')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple tuple(x uint64)')" 2>&1 | grep -q -F "INCORRECT_DATA" && echo "OK" || echo "FAIL"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple tuple(x uint64, b string)')" 2>&1 | grep -q -F "INCORRECT_DATA" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'tuple tuple(uint64, string)') select tuple(number, 'Hello') from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple tuple(x uint64, s string)')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple tuple(uint64, string)')"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple tuple(uint64)')" 2>&1 | grep -q -F "INCORRECT_DATA" && echo "OK" || echo "FAIL"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'tuple tuple(uint64, string, uint64)')" 2>&1 | grep -q -F "INCORRECT_DATA" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "map"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'map map(uint64, uint64)') select map(1, number, 2, number + 1) from numbers(5) settings engine_file_truncate_on_insert=1" 2>&1 | grep -q -F "ILLEGAL_COLUMN" && echo "OK" || echo "FAIL"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'map map(string, uint64)') select map('a', number, 'b', number + 1) from numbers(5) settings engine_file_truncate_on_insert=1"

$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'map map(uint64, uint64)')" 2>&1 | grep -q -F "ILLEGAL_COLUMN" && echo "OK" || echo "FAIL"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'map map(string, uint64)')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "Nested types"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'nested1 array(array(uint32)), nested2 tuple(tuple(x uint32, s string), string), nested3 map(string, map(string, uint32))') select [range(number), range(number + 1)], tuple(tuple(number, 'Hello'), 'Hello'), map('a', map('a.a', number, 'a.b', number + 1), 'b', map('b.a', number, 'b.b', number + 1)) from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'nested1 array(array(uint32)), nested2 tuple(tuple(x uint32, s string), string), nested3 map(string, map(string, uint32))')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow, auto, 'nested array(tuple(map(string, array(uint32)), array(map(string, tuple(array(uint64), array(uint64))))))') select [(map('a', range(number), 'b', range(number + 1)), [map('c', (range(number), range(number + 1))), map('d', (range(number + 2), range(number + 3)))])] from numbers(5) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow, auto, 'nested array(tuple(map(string, array(uint32)), array(map(string, tuple(array(uint64), array(uint64))))))')"

$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "select * from file(02475_data.bsonEachRow)"


echo "Schema inference"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select number::bool as x from numbers(2) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select number::int32 as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select number::uint32 as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select number::int64 as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select to_string(number) as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)" 2>&1 | grep -q -F "TYPE_MISMATCH" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [number::bool] as x from numbers(2) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [number::int32] as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [number::uint32] as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [number::int64] as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [to_string(number)] as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)" 2>&1 | grep -q -F "TYPE_MISMATCH" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [] as x from numbers(2) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)" 2>&1 | grep -q -F "ONLY_NULLS_WHILE_READING_SCHEMA" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select NULL as x from numbers(2) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)" 2>&1 | grep -q -F "ONLY_NULLS_WHILE_READING_SCHEMA" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select [NULL, 1] as x from numbers(2) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)" 2>&1 | grep -q -F "ONLY_NULLS_WHILE_READING_SCHEMA" && echo "OK" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select tuple(1, 'str') as x from numbers(2) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "insert into function file(02475_data.bsonEachRow) select tuple(1) as x from numbers(2)"
$CLICKHOUSE_CLIENT -q "desc file(02475_data.bsonEachRow)" 2>&1 | grep -q -F "TYPE_MISMATCH" && echo "OK" || echo "FAIL"


echo "Sync after error"
$CLICKHOUSE_CLIENT -q "insert into function file(data.bsonEachRow) select number, 42::int128 as int, range(number) as arr from numbers(3) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q " insert into function file(data.bsonEachRow) select number, 'Hello' as int, range(number) as arr from numbers(2) settings engine_file_truncate_on_insert=0"
$CLICKHOUSE_CLIENT -q "insert into function file(data.bsonEachRow) select number, 42::int128 as int, range(number) as arr from numbers(3) settings engine_file_truncate_on_insert=0"
$CLICKHOUSE_CLIENT -q "select * from file(data.bsonEachRow, auto, 'number uint64, int int128, arr array(uint64)') settings input_format_allow_errors_num=0"  2>&1 | grep -q -F "INCORRECT_DATA" && echo "OK" || echo "FAIL"
$CLICKHOUSE_CLIENT -q "select * from file(data.bsonEachRow, auto, 'number uint64, int int128, arr array(uint64)') settings input_format_allow_errors_num=2"
