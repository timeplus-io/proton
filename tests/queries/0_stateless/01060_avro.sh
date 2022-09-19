#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro

# input
echo '===' input
echo '=' primitive

cat "$DATA_DIR"/primitive.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S 'a_bool uint8, b_int int32, c_long int64, d_float Float32, e_double float64, f_bytes string, g_string string' -q 'select * from table'
cat "$DATA_DIR"/primitive.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S 'a_bool uint8, c_long int64, g_string string' -q 'select * from table'
cat "$DATA_DIR"/primitive.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S 'g_string string, c_long int64, a_bool uint8' -q 'select * from table'
cat "$DATA_DIR"/primitive.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S 'g_string string' -q 'select * from table'

echo '=' complex
cat "$DATA_DIR"/complex.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "a_enum_to_string string, b_enum_to_enum Enum('t' = 1, 'f' = 0), c_array_string array(string), d_array_array_string array(array(string)), e_union_null_string Nullable(string), f_union_long_null Nullable(int64), g_fixed FixedString(32)" -q 'select * from table'
cat "$DATA_DIR"/complex.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "g_fixed FixedString(32)" -q 'select * from table'

echo '=' logical_types
cat "$DATA_DIR"/logical_types.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "a_date date, b_timestamp_millis DateTime64(3, 'UTC'), c_timestamp_micros DateTime64(6, 'UTC'), d_uuid UUID" -q 'select * from table'
cat "$DATA_DIR"/logical_types.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S 'a_date int32, b_timestamp_millis int64, c_timestamp_micros int64, d_uuid UUID' -q 'select * from table'

echo '=' references
cat "$DATA_DIR"/references.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "a string, c string" -q 'select * from table'

echo '=' nested
cat "$DATA_DIR"/nested.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S 'a int64, "b.a" string, "b.b" Double, "b.c" Double, c string' -q 'select * from table'
cat "$DATA_DIR"/nested.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S '"b.c" Double, "b.a" string, a int64, c string' -q 'select * from table'
cat "$DATA_DIR"/nested.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S '"b" Double' -q 'select * from table' 2>&1 | grep -i 'not compatible' -o

echo '=' nested_complex
# special case union(null, T)
cat "$DATA_DIR"/nested_complex.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S '"b.b2_null_str" Nullable(string)' -q 'select * from table'
# union branch to non-null with default
cat "$DATA_DIR"/nested_complex.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "\"b.b2_null_str.string\" string default 'default'"    -q 'select * from table'
# union branch to nullable
cat "$DATA_DIR"/nested_complex.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "\"b.b2_null_str.string\" Nullable(string)"    -q 'select * from table'
# multiple union branches simultaneously
cat "$DATA_DIR"/nested_complex.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "\"b.b3_null_str_double.string\" Nullable(string), \"b.b3_null_str_double.double\" Nullable(Double)"    -q 'select * from table'
# and even nested recursive structures!
cat "$DATA_DIR"/nested_complex.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "\"b.b4_null_sub1.sub1.b2_null_str\" Nullable(string)"    -q 'select * from table'

echo '=' compression
cat "$DATA_DIR"/simple.null.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S 'a int64' -q 'select count() from table'
cat "$DATA_DIR"/simple.deflate.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S 'a int64' -q 'select count() from table'

#snappy is optional
#cat $DATA_DIR/simple.snappy.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S 'a int64' -q 'select count() from table'

echo '=' other
#no data
cat "$DATA_DIR"/empty.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S 'a int64' -q 'select count() from table'
# type mismatch
cat "$DATA_DIR"/simple.null.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S 'a int32' -q 'select count() from table'
# field not found
cat "$DATA_DIR"/simple.null.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S 'b int64' -q 'select count() from table' 2>&1 | grep -i 'not found' -o
# allow_missing_fields
cat "$DATA_DIR"/simple.null.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV --input_format_avro_allow_missing_fields 1 -S 'b int64' -q 'select count() from table'






# output
echo '===' output

echo '=' primitive
S1="a_bool uint8, b_int int32, c_long int64, d_float Float32, e_double float64, f_bytes string, g_string string"
echo '1,1,2,3.4,5.6,"b1","s1"' | ${CLICKHOUSE_LOCAL} --input-format CSV -S "$S1" -q "select * from table  format Avro" | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "$S1" -q 'select * from table'

echo '=' complex
S2="a_enum_to_string string, b_enum_to_enum Enum('t' = 1, 'f' = 0), c_array_string array(string), d_array_array_string array(array(string)), e_union_null_string Nullable(string), f_union_long_null Nullable(int64), g_fixed FixedString(32)"
echo "\"A\",\"t\",\"['s1','s2']\",\"[['a1'],['a2']]\",\"s1\",\N,\"79cd909892d7e7ade1987cc7422628ba\"" | ${CLICKHOUSE_LOCAL} --input-format CSV -S "$S2" -q "select * from table  format Avro" | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "$S2" -q 'select * from table'

echo '=' logical_types
S3="a_date date, b_timestamp_millis DateTime64(3, 'UTC'), c_timestamp_micros DateTime64(6, 'UTC'), d_uuid UUID"
echo '"2019-12-20","2020-01-10 07:31:56.227","2020-01-10 07:31:56.227000","7c856fd6-005f-46c7-a7b5-3a082ef6c659"' | ${CLICKHOUSE_LOCAL} --input-format CSV -S "$S3" -q "select * from table  format Avro" | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "$S3" -q 'select * from table'

echo '=' other
S4="a int64"
${CLICKHOUSE_LOCAL} -q "select to_int64(number) as a from numbers(0)  format Avro" | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "$S4" -q 'select count() from table'
${CLICKHOUSE_LOCAL} -q "select to_int64(number) as a from numbers(1000)  format Avro" | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "$S4" -q 'select count() from table'

# type supported via conversion
${CLICKHOUSE_LOCAL}  -q "select to_int16(123) as a format Avro" | wc -c | tr -d ' '

echo '=' string column pattern
${CLICKHOUSE_LOCAL} -q "select 'русская строка' as a  format Avro SETTINGS output_format_avro_string_column_pattern = 'a'" | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "a string" -q 'select * from table'

# it is expected that invalid UTF-8 can be created
${CLICKHOUSE_LOCAL} -q "select '\x61\xF0\x80\x80\x80b' as a  format Avro" > /dev/null && echo Ok

A_NEEDLE="'\"name\":\"a\",\"type\":\"string\"'"
AAA_NEEDLE="'\"name\":\"aaa\",\"type\":\"string\"'"
B_NEEDLE="'\"name\":\"b\",\"type\":\"string\"'"
PATTERNQUERY="select 'русская строка' as a, 'русская строка' as aaa, 'русская строка' as b format Avro SETTINGS output_format_avro_string_column_pattern ="

PATTERNPATTERN="'a'"
${CLICKHOUSE_LOCAL} -q "$PATTERNQUERY $PATTERNPATTERN" | tr -d '\n' | ${CLICKHOUSE_LOCAL} --structure "avro_raw string" --input-format LineAsString  -q "select countSubstrings(avro_raw, $A_NEEDLE), countSubstrings(avro_raw, $AAA_NEEDLE), countSubstrings(avro_raw, $B_NEEDLE) from table"

PATTERNPATTERN="'^a$'"
${CLICKHOUSE_LOCAL} -q "$PATTERNQUERY $PATTERNPATTERN" | tr -d '\n' | ${CLICKHOUSE_LOCAL} --structure "avro_raw string" --input-format LineAsString  -q "select countSubstrings(avro_raw, $A_NEEDLE), countSubstrings(avro_raw, $AAA_NEEDLE), countSubstrings(avro_raw, $B_NEEDLE) from table"

PATTERNPATTERN="'aaa'"
${CLICKHOUSE_LOCAL} -q "$PATTERNQUERY $PATTERNPATTERN" | tr -d '\n' | ${CLICKHOUSE_LOCAL} --structure "avro_raw string" --input-format LineAsString  -q "select countSubstrings(avro_raw, $A_NEEDLE), countSubstrings(avro_raw, $AAA_NEEDLE), countSubstrings(avro_raw, $B_NEEDLE) from table"

PATTERNPATTERN="'a|b'"
${CLICKHOUSE_LOCAL} -q "$PATTERNQUERY $PATTERNPATTERN" | tr -d '\n' | ${CLICKHOUSE_LOCAL} --structure "avro_raw string" --input-format LineAsString  -q "select countSubstrings(avro_raw, $A_NEEDLE), countSubstrings(avro_raw, $AAA_NEEDLE), countSubstrings(avro_raw, $B_NEEDLE) from table"

PATTERNPATTERN="'.*'"
${CLICKHOUSE_LOCAL} -q "$PATTERNQUERY $PATTERNPATTERN" | tr -d '\n' | ${CLICKHOUSE_LOCAL} --structure "avro_raw string" --input-format LineAsString  -q "select countSubstrings(avro_raw, $A_NEEDLE), countSubstrings(avro_raw, $AAA_NEEDLE), countSubstrings(avro_raw, $B_NEEDLE) from table"
