#!/usr/bin/env bash

#--------------------------------------------
# Description of test result:
#   Test the correctness of the optimization
#   by asserting read marks in the log.
# Relation of read marks and optimization:
#   read marks =
#       the number of monotonic marks filtered through predicates
#       + no monotonic marks count
#--------------------------------------------

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS string_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS fixed_string_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS signed_integer_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS unsigned_integer_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS enum_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS date_test_table;"

${CLICKHOUSE_CLIENT} --query="create stream string_test_table (val string) ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;"
${CLICKHOUSE_CLIENT} --query="create stream fixed_string_test_table (val fixed_string(1)) ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;"
${CLICKHOUSE_CLIENT} --query="create stream signed_integer_test_table (val int32) ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;"
${CLICKHOUSE_CLIENT} --query="create stream unsigned_integer_test_table (val uint32) ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;"
${CLICKHOUSE_CLIENT} --query="create stream enum_test_table (val Enum16('hello' = 1, 'world' = 2, 'yandex' = 256, 'clickhouse' = 257)) ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;"
${CLICKHOUSE_CLIENT} --query="create stream date_test_table (val date) ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;"

${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES string_test_table;"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES fixed_string_test_table;"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES signed_integer_test_table;"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES unsigned_integer_test_table;"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES enum_test_table;"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES date_test_table;"

${CLICKHOUSE_CLIENT} --query="INSERT INTO string_test_table VALUES ('0'), ('2'), ('2');"
${CLICKHOUSE_CLIENT} --query="INSERT INTO fixed_string_test_table VALUES ('0'), ('2'), ('2');"
# 131072 -> 17 bit is 1
${CLICKHOUSE_CLIENT} --query="INSERT INTO signed_integer_test_table VALUES (-2), (0), (2), (2), (131072), (131073), (131073);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO unsigned_integer_test_table VALUES (0), (2), (2), (131072), (131073), (131073);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO enum_test_table VALUES ('hello'), ('world'), ('world'), ('yandex'), ('clickhouse'), ('clickhouse');"
${CLICKHOUSE_CLIENT} --query="INSERT INTO date_test_table VALUES (1), (2), (2), (256), (257), (257);"

CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=debug/g')

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM string_test_table WHERE to_uint64(val) == 0;" 2>&1 |grep -q "3 marks to read from 1 ranges" && echo "no monotonic int case: string -> uint64"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM fixed_string_test_table WHERE to_uint64(val) == 0;" 2>&1 |grep -q "3 marks to read from 1 ranges" && echo "no monotonic int case: fixed_string -> uint64"

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM signed_integer_test_table WHERE to_int64(val) == 0;" 2>&1 |grep -q "2 marks to read from" && echo "monotonic int case: int32 -> int64"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM signed_integer_test_table WHERE to_uint64(val) == 0;" 2>&1 |grep -q "2 marks to read from" && echo "monotonic int case: int32 -> uint64"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM signed_integer_test_table WHERE to_int32(val) == 0;" 2>&1 |grep -q "2 marks to read from" && echo "monotonic int case: int32 -> int32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM signed_integer_test_table WHERE to_uint32(val) == 0;" 2>&1 |grep -q "2 marks to read from" && echo "monotonic int case: int32 -> uint32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM signed_integer_test_table WHERE to_int16(val) == 0;" 2>&1 |grep -q "5 marks to read from" && echo "monotonic int case: int32 -> int16"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM signed_integer_test_table WHERE to_uint16(val) == 0;" 2>&1 |grep -q "5 marks to read from" && echo "monotonic int case: int32 -> uint16"

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM unsigned_integer_test_table WHERE to_int64(val) == 0;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: uint32 -> int64"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM unsigned_integer_test_table WHERE to_uint64(val) == 0;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: uint32 -> uint64"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM unsigned_integer_test_table WHERE to_int32(val) == 0;" 2>&1 |grep -q "2 marks to read from" && echo "monotonic int case: uint32 -> int32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM unsigned_integer_test_table WHERE to_uint32(val) == 0;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: uint32 -> uint32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM unsigned_integer_test_table WHERE to_int16(val) == 0;" 2>&1 |grep -q "4 marks to read from" && echo "monotonic int case: uint32 -> int16"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM unsigned_integer_test_table WHERE to_uint16(val) == 0;" 2>&1 |grep -q "4 marks to read from" && echo "monotonic int case: uint32 -> uint16"


${CLICKHOUSE_CLIENT} --query="SELECT count() FROM enum_test_table WHERE to_int32(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: Enum16 -> int32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM enum_test_table WHERE to_uint32(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: Enum16 -> uint32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM enum_test_table WHERE to_int16(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: Enum16 -> int16"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM enum_test_table WHERE to_uint16(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: Enum16 -> uint16"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM enum_test_table WHERE to_int8(val) == 1;" 2>&1 |grep -q "5 marks to read from" && echo "monotonic int case: Enum16 -> int8"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM enum_test_table WHERE to_uint8(val) == 1;" 2>&1 |grep -q "5 marks to read from" && echo "monotonic int case: Enum16 -> uint8"


${CLICKHOUSE_CLIENT} --query="SELECT count() FROM date_test_table WHERE to_int32(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: date -> int32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM date_test_table WHERE to_uint32(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: date -> uint32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM date_test_table WHERE to_int16(val) == 1;" 2>&1 |grep -q "2 marks to read from" && echo "monotonic int case: date -> int16"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM date_test_table WHERE to_uint16(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: date -> uint16"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM date_test_table WHERE to_int8(val) == 1;" 2>&1 |grep -q "5 marks to read from" && echo "monotonic int case: date -> int8"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM date_test_table WHERE to_uint8(val) == 1;" 2>&1 |grep -q "5 marks to read from" && echo "monotonic int case: date -> uint8"

CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/--send_logs_level=debug/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/g')

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS string_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS fixed_string_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS signed_integer_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS unsigned_integer_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS enum_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS date_test_table;"
