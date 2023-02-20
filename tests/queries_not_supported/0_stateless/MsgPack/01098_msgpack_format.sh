#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')


$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS msgpack";

$CLICKHOUSE_CLIENT --query="CREATE STREAM msgpack (uint8 uint8, uint16 uint16, uint32 uint32, uint64 uint64, int8 int8, int16 int16, int32 int32, int64 int64, float float32, double float64, string string, date Date, datetime DateTime('Asia/Istanbul'), datetime64 DateTime64(3, 'Asia/Istanbul'), array array(uint32)) ENGINE = Memory";


$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack VALUES (255, 65535, 4294967295, 100000000000, -128, -32768, -2147483648, -100000000000, 2.02, 10000.0000001, 'string', 18980, 1639872000, 1639872000000, [1,2,3,4,5]), (4, 1234, 3244467295, 500000000000, -1, -256, -14741221, -7000000000, 100.1, 14321.032141201, 'Another string', 20000, 1839882000, 1639872891123, [5,4,3,2,1]), (42, 42, 42, 42, 42, 42, 42, 42, 42.42, 42.42, '42', 42, 42, 42, [42])";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack FORMAT MsgPack" > "$CURDIR"/tmp_msgpac_test_all_types.msgpk;

cat "$CURDIR"/tmp_msgpac_test_all_types.msgpk | $CLICKHOUSE_CLIENT --query="INSERT INTO msgpack FORMAT MsgPack";

rm  "$CURDIR"/tmp_msgpac_test_all_types.msgpk

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack";

$CLICKHOUSE_CLIENT --query="DROP STREAM msgpack";


$CLICKHOUSE_CLIENT --query="CREATE STREAM msgpack (array1 array(array(uint32)), array2 array(array(array(string)))) ENGINE = Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack VALUES ([[1,2,3], [1001, 2002], [3167]], [[['one'], ['two']], [['three']],[['four'], ['five']]])";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack FORMAT MsgPack" > "$CURDIR"/tmp_msgpack_test_nested_arrays.msgpk;

cat "$CURDIR"/tmp_msgpack_test_nested_arrays.msgpk | $CLICKHOUSE_CLIENT --query="INSERT INTO msgpack FORMAT MsgPack";
rm "$CURDIR"/tmp_msgpack_test_nested_arrays.msgpk;

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack";

$CLICKHOUSE_CLIENT --query="DROP STREAM msgpack";


$CLICKHOUSE_CLIENT --query="CREATE STREAM msgpack (array array(uint8)) ENGINE = Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack FORMAT MsgPack" > "$CURDIR"/tmp_msgpack_type_conversion.msgpk;

$CLICKHOUSE_CLIENT --query="DROP STREAM msgpack";

$CLICKHOUSE_CLIENT --query="CREATE STREAM msgpack (array array(int64)) ENGINE = Memory";

cat "$CURDIR"/tmp_msgpack_type_conversion.msgpk | $CLICKHOUSE_CLIENT --query="INSERT INTO msgpack FORMAT MsgPack";
rm "$CURDIR"/tmp_msgpack_type_conversion.msgpk;

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack";

$CLICKHOUSE_CLIENT --query="DROP STREAM msgpack";

$CLICKHOUSE_CLIENT --query="CREATE STREAM msgpack (date fixed_string(10)) ENGINE = Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack VALUES ('2020-01-01'), ('2020-01-02'), ('2020-01-02')";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack";

$CLICKHOUSE_CLIENT --query="DROP STREAM msgpack";


$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS msgpack_map";

$CLICKHOUSE_CLIENT --query="CREATE STREAM msgpack_map (m map(uint64, uint64), a array(map(uint64, array(uint64)))) ENGINE=Memory()";

$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack_map VALUES ({1 : 2, 2 : 3}, [{1 : [1, 2], 2 : [3, 4]}, {3 : [5, 6], 4 : [7, 8]}])";


$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack_map FORMAT MsgPack" | $CLICKHOUSE_CLIENT --query="INSERT INTO msgpack_map FORMAT MsgPack";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack_map";

$CLICKHOUSE_CLIENT --query="DROP STREAM msgpack_map";


$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS msgpack_lc_nullable";

$CLICKHOUSE_CLIENT --query="CREATE STREAM msgpack_lc_nullable (a low_cardinality(string), b nullable(string), c low_cardinality(nullable(string)), d array(nullable(string)), e array(low_cardinality(nullable(string)))) engine=Memory()";

$CLICKHOUSE_CLIENT --query="INSERT INTO msgpack_lc_nullable VALUES ('42', '42', '42', ['42', '42'], ['42', '42']), ('42', NULL, NULL, [NULL, '42', NULL], [NULL, '42', NULL])";


$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack_lc_nullable FORMAT MsgPack" | $CLICKHOUSE_CLIENT --query="INSERT INTO msgpack_lc_nullable FORMAT MsgPack";

$CLICKHOUSE_CLIENT --query="SELECT * FROM msgpack_lc_nullable";

$CLICKHOUSE_CLIENT --query="DROP STREAM msgpack_lc_nullable";


$CLICKHOUSE_CLIENT --query="SELECT to_string(number) FROM  numbers(10) FORMAT MsgPack" > $USER_FILES_PATH/data.msgpack

$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x uint64')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x float32')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x array(uint32)')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x map(uint64, uint64)')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';


$CLICKHOUSE_CLIENT --query="SELECT number FROM  numbers(10) FORMAT MsgPack" > $USER_FILES_PATH/data.msgpack

$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x float32')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x string')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x array(uint64)')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x map(uint64, uint64)')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';


$CLICKHOUSE_CLIENT --query="SELECT [number, number + 1] FROM  numbers(10) FORMAT MsgPack" > $USER_FILES_PATH/data.msgpack

$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x float32')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x string')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x uint64')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x map(uint64, uint64)')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';


$CLICKHOUSE_CLIENT --query="SELECT map(number, number + 1) FROM  numbers(10) FORMAT MsgPack" > $USER_FILES_PATH/data.msgpack

$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x float32')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x string')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x uint64')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.msgpack', 'MsgPack', 'x array(uint64)')" 2>&1 | grep -F -q "ILLEGAL_COLUMN" && echo 'OK' || echo 'FAIL';


rm $USER_FILES_PATH/data.msgpack

