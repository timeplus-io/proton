#!/usr/bin/env bash
# Tags: long, no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS contributors"
${CLICKHOUSE_CLIENT} --query="create stream contributors (name string) "
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.contributors ORDER BY name DESC FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO contributors FORMAT Parquet"
# random results
${CLICKHOUSE_CLIENT} --query="SELECT * FROM contributors LIMIT 10" > /dev/null
${CLICKHOUSE_CLIENT} --query="DROP STREAM contributors"

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_numbers"
${CLICKHOUSE_CLIENT} --query="create stream parquet_numbers (number uint64) "
# less than default block size (65k)
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 10000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_numbers ORDER BY number DESC LIMIT 10"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE parquet_numbers"

# More than default block size
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 100000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_numbers ORDER BY number DESC LIMIT 10"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE parquet_numbers"

${CLICKHOUSE_CLIENT} --max_block_size=2 --query="SELECT * FROM system.numbers LIMIT 3 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_numbers ORDER BY number DESC LIMIT 10"

${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE parquet_numbers"
${CLICKHOUSE_CLIENT} --max_block_size=1 --query="SELECT * FROM system.numbers LIMIT 1000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_numbers ORDER BY number DESC LIMIT 10"

${CLICKHOUSE_CLIENT} --query="DROP STREAM parquet_numbers"

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_events"
${CLICKHOUSE_CLIENT} --query="create stream parquet_events (event string, value uint64, description string) "
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.events FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_events FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT event, description FROM parquet_events WHERE event IN ('ContextLock', 'Query') ORDER BY event"
${CLICKHOUSE_CLIENT} --query="DROP STREAM parquet_events"

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_types1"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_types2"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_types3"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_types4"
${CLICKHOUSE_CLIENT} --query="create stream parquet_types1       (int8 int8, uint8 uint8, int16 int16, uint16 uint16, int32 int32, uint32 uint32, int64 int64, uint64 uint64, float32 float32, float64 float64, string string, fixedstring fixed_string(15), date date, datetime datetime) "
${CLICKHOUSE_CLIENT} --query="create stream parquet_types2       (int8 int8, uint8 uint8, int16 int16, uint16 uint16, int32 int32, uint32 uint32, int64 int64, uint64 uint64, float32 float32, float64 float64, string string, fixedstring fixed_string(15), date date, datetime datetime) "
# convert min type
${CLICKHOUSE_CLIENT} --query="create stream parquet_types3       (int8 int8,  uint8 int8,  int16 int8,   uint16 int8,  int32 int8,   uint32 int8,  int64 int8,   uint64 int8,    float32 int8,    float64 int8, string fixed_string(15), fixedstring fixed_string(15), date date,    datetime date) "
# convert max type
${CLICKHOUSE_CLIENT} --query="create stream parquet_types4       (int8 int64, uint8 int64, int16 int64, uint16 int64, int32 int64,  uint32 int64, int64 int64,  uint64 int64,   float32 int64,   float64 int64, string string,          fixedstring string, date datetime, datetime datetime) "

${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types1 values (     -108,         108,       -1016,          1116,       -1032,          1132,       -1064,          1164,          -1.032,          -1.064,    'string-0',               'fixedstring', '2001-02-03', '2002-02-03 04:05:06')"

# min
${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types1 values (     -128,           0,      -32768,             0, -2147483648,             0, -9223372036854775808, 0,             -1.032,          -1.064,    'string-1',             'fixedstring-1', '2003-04-05', '2003-02-03 04:05:06')"

# max
${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types1 values (      127,         255,       32767,         65535,  2147483647,    4294967295, 9223372036854775807, 9223372036854775807, -1.032,     -1.064,    'string-2',             'fixedstring-2', '2004-06-07', '2004-02-03 04:05:06')"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types2 FORMAT Parquet"

echo original:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 ORDER BY int8" | tee "${CLICKHOUSE_TMP}"/parquet_all_types_1.dump
echo converted:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types2 ORDER BY int8" | tee "${CLICKHOUSE_TMP}"/parquet_all_types_2.dump
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 ORDER BY int8 FORMAT Parquet" > "${CLICKHOUSE_TMP}"/parquet_all_types_1.parquet
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types2 ORDER BY int8 FORMAT Parquet" > "${CLICKHOUSE_TMP}"/parquet_all_types_2.parquet
echo diff:
diff "${CLICKHOUSE_TMP}"/parquet_all_types_1.dump "${CLICKHOUSE_TMP}"/parquet_all_types_2.dump

${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE parquet_types2"
${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types3 values (       79,          81,          82,            83,          84,            85,          86,            87,              88,              89,         'str01',                  'fstr1', '2003-03-04', '2004-05-06')"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types3 ORDER BY int8 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types2 FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 ORDER BY int8 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types3 FORMAT Parquet"

${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types4 values (       80,          81,          82,            83,          84,            85,          86,            87,              88,              89,         'str02',                  'fstr2', '2005-03-04 05:06:07', '2006-08-09 10:11:12')"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types4 ORDER BY int8 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types2 FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 ORDER BY int8 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types4 FORMAT Parquet"

echo dest:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types2 ORDER BY int8"
echo min:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types3 ORDER BY int8"
echo max:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types4 ORDER BY int8"


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_types5"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_types6"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE parquet_types2"
${CLICKHOUSE_CLIENT} --query="create stream parquet_types5       (int8 nullable(int8), uint8 nullable(uint8), int16 nullable(int16), uint16 nullable(uint16), int32 nullable(int32), uint32 nullable(uint32), int64 nullable(int64), uint64 nullable(uint64), float32 nullable(float32), float64 nullable(float64), string nullable(string), fixedstring nullable(fixed_string(15)), date nullable(date), datetime nullable(datetime)) "
${CLICKHOUSE_CLIENT} --query="create stream parquet_types6       (int8 nullable(int8), uint8 nullable(uint8), int16 nullable(int16), uint16 nullable(uint16), int32 nullable(int32), uint32 nullable(uint32), int64 nullable(int64), uint64 nullable(uint64), float32 nullable(float32), float64 nullable(float64), string nullable(string), fixedstring nullable(fixed_string(15)), date nullable(date), datetime nullable(datetime)) "
${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types5 values (               NULL,                  NULL,                  NULL,                    NULL,                  NULL,                    NULL,                  NULL,                    NULL,                      NULL,                      NULL,                    NULL,                                  NULL,                NULL,                        NULL)"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types5 ORDER BY int8 FORMAT Parquet" > "${CLICKHOUSE_TMP}"/parquet_all_types_5.parquet
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types5 ORDER BY int8 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types6 FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types1 ORDER BY int8 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_types6 FORMAT Parquet"
echo dest from null:
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_types6 ORDER BY int8"


${CLICKHOUSE_CLIENT} --query="DROP STREAM parquet_types5"
${CLICKHOUSE_CLIENT} --query="DROP STREAM parquet_types6"


${CLICKHOUSE_CLIENT} --query="DROP STREAM parquet_types1"
${CLICKHOUSE_CLIENT} --query="DROP STREAM parquet_types2"
${CLICKHOUSE_CLIENT} --query="DROP STREAM parquet_types3"
${CLICKHOUSE_CLIENT} --query="DROP STREAM parquet_types4"


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_arrays"

${CLICKHOUSE_CLIENT} --query="create stream parquet_arrays (id uint32, a1 array(int8), a2 array(uint8), a3 array(int16), a4 array(uint16), a5 array(int32), a6 array(uint32), a7 array(int64), a8 array(uint64), a9 array(string), a10 array(fixed_string(4)), a11 array(float32), a12 array(float64), a13 array(date), a14 array(datetime), a15 array(Decimal(4, 2)), a16 array(Decimal(10, 2)), a17 array(Decimal(25, 2))) engine=Memory()"

${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_arrays VALUES (1, [1,-2,3], [1,2,3], [100, -200, 300], [100, 200, 300], [10000000, -20000000, 30000000], [10000000, 2000000, 3000000], [100000000000000, -200000000000, 3000000000000], [100000000000000, 20000000000000, 3000000000000], ['Some string', 'Some string', 'Some string'], ['0000', '1111', '2222'], [42.42, 424.2, 0.4242], [424242.424242, 4242042420.242424, 42], ['2000-01-01', '2001-01-01', '2002-01-01'], ['2000-01-01', '2001-01-01', '2002-01-01'], [0.2, 10.003, 4.002], [4.000000001, 10000.10000, 10000.100001], [1000000000.000000001123, 90.0000000010010101, 0101001.0112341001])"

${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_arrays VALUES (2, [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [])"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_arrays FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_arrays FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_arrays ORDER BY id"

${CLICKHOUSE_CLIENT} --query="DROP STREAM parquet_arrays"


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_nullable_arrays"
${CLICKHOUSE_CLIENT} --query="create stream parquet_nullable_arrays (id uint32, a1 array(nullable(uint32)), a2 array(nullable(string)), a3 array(nullable(Decimal(4, 2)))) engine=Memory()"
${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_nullable_arrays VALUES (1, [1, Null, 2], [Null, 'Some string', Null], [0.001, Null, 42.42]), (2, [Null], [Null], [Null]), (3, [], [], [])"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_nullable_arrays FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_nullable_arrays FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_nullable_arrays ORDER BY id"
${CLICKHOUSE_CLIENT} --query="DROP STREAM parquet_nullable_arrays"


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_nested_arrays"
${CLICKHOUSE_CLIENT} --query="create stream parquet_nested_arrays (a1 array(array(array(uint32))), a2 array(array(array(string))), a3 array(array(nullable(uint32))), a4 array(array(nullable(string)))) engine=Memory() "
${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_nested_arrays VALUES ([[[1,2,3], [1,2,3]], [[1,2,3]], [[], [1,2,3]]], [[['Some string', 'Some string'], []], [['Some string']], [[]]], [[Null, 1, 2], [Null], [1, 2], []], [['Some string', Null, 'Some string'], [Null], []])"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_nested_arrays FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_nested_arrays FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_nested_arrays"
${CLICKHOUSE_CLIENT} --query="DROP STREAM parquet_nested_arrays"


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_decimal"
${CLICKHOUSE_CLIENT} --query="create stream parquet_decimal (d1 Decimal32(4), d2 Decimal64(8), d3 Decimal128(16), d4 Decimal256(32)) "
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE parquet_decimal VALUES (0.123, 0.123123123, 0.123123123123, 0.123123123123123123)"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_decimal FORMAT Arrow" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_decimal FORMAT Arrow"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_decimal"
${CLICKHOUSE_CLIENT} --query="DROP STREAM parquet_decimal"
