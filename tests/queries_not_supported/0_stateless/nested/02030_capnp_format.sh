#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
CAPN_PROTO_FILE=$USER_FILES_PATH/data.capnp
touch $CAPN_PROTO_FILE

SCHEMADIR=$(clickhouse-client --query "select * from file('data.capnp', 'CapnProto', 'val1 char') settings format_schema='nonexist:Message'" 2>&1 | grep Exception | grep -oP "file \K.*(?=/nonexist.capnp)")
CLIENT_SCHEMADIR=$CURDIR/format_schemas
SERVER_SCHEMADIR=test_02030
mkdir -p $SCHEMADIR/$SERVER_SCHEMADIR
cp -r $CLIENT_SCHEMADIR/02030_* $SCHEMADIR/$SERVER_SCHEMADIR/


$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS capnp_simple_types";
$CLICKHOUSE_CLIENT --query="create stream capnp_simple_types (int8 int8, uint8 uint8, int16 Int16, uint16 UInt16, int32 int32, uint32 uint32, int64 Int64, uint64 uint64, float32 Float32, float64 float64, string string, fixed FixedString(5), data string, date date, datetime DateTime, datetime64 DateTime64(3)) ENGINE=Memory"
$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_simple_types values (-1, 1, -1000, 1000, -10000000, 1000000, -1000000000, 1000000000, 123.123, 123123123.123123123, 'Some string', 'fixed', 'Some data', '2000-01-06', '2000-06-01 19:42:42', '2000-04-01 11:21:33.123')"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_simple_types FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_simple_types:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_simple_types FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_simple_types:Message'"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_simple_types"
$CLICKHOUSE_CLIENT --query="DROP STREAM capnp_simple_types"


$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS capnp_tuples"
$CLICKHOUSE_CLIENT --query="create stream capnp_tuples (value uint64, tuple1 tuple(one uint64, two tuple(three uint64, four uint64)), tuple2 tuple(nested1 tuple(nested2 tuple(x uint64)))) ENGINE=Memory";
$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_tuples VALUES (1, (2, (3, 4)), (((5))))"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_tuples FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_tuples:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_tuples FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_tuples:Message'"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_tuples"
$CLICKHOUSE_CLIENT --query="DROP STREAM capnp_tuples"


$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS capnp_lists"
$CLICKHOUSE_CLIENT --query="create stream capnp_lists (value uint64, list1 array(uint64), list2 array(array(array(uint64)))) ENGINE=Memory";
$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_lists VALUES (1, [1, 2, 3], [[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], []], []])"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_lists FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_lists:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_lists FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_lists:Message'"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_lists"
$CLICKHOUSE_CLIENT --query="DROP STREAM capnp_lists"


$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS capnp_nested_lists_and_tuples"
$CLICKHOUSE_CLIENT --query="create stream capnp_nested_lists_and_tuples (value uint64, nested tuple(a tuple(b uint64, c array(array(uint64))), d array(tuple(e array(array(tuple(f uint64, g uint64))), h array(tuple(k array(uint64))))))) ENGINE=Memory";
$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_nested_lists_and_tuples VALUES (1, ((2, [[3, 4], [5, 6], []]), [([[(7, 8), (9, 10)], [(11, 12), (13, 14)], []], [([15, 16, 17]), ([])])]))"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_nested_lists_and_tuples FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_nested_lists_and_tuples:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_nested_lists_and_tuples FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_nested_lists_and_tuples:Message'"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_nested_lists_and_tuples"
$CLICKHOUSE_CLIENT --query="DROP STREAM capnp_nested_lists_and_tuples"


$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS capnp_nested_table"
$CLICKHOUSE_CLIENT --query="create stream capnp_nested_table (nested nested(value uint64, array array(uint64), tuple tuple(one uint64, two uint64))) ENGINE=Memory";
$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_nested_table VALUES ([1, 2, 3], [[4, 5, 6], [], [7, 8]], [(9, 10), (11, 12), (13, 14)])"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_nested_table FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_nested_table:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_nested_table FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_nested_table:Message'"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_nested_table"
$CLICKHOUSE_CLIENT --query="DROP STREAM capnp_nested_table"


$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS capnp_nullable"
$CLICKHOUSE_CLIENT --query="create stream capnp_nullable (nullable Nullable(uint64), array array(Nullable(uint64)), tuple tuple(nullable Nullable(uint64))) ENGINE=Memory";
$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_nullable VALUES (1, [1, Null, 2], (1)), (Null, [Null, Null, 42], (Null))"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_nullable FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_nullable:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_nullable FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_nullable:Message'"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_nullable"
$CLICKHOUSE_CLIENT --query="DROP STREAM capnp_nullable"


$CLICKHOUSE_CLIENT --query="SELECT CAST(number, 'Enum(\'one\' = 0, \'two\' = 1, \'tHrEe\' = 2)') AS value FROM numbers(3) FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_enum:Message'" > $CAPN_PROTO_FILE

$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'value Enum(\'one\' = 1, \'two\' = 2, \'tHrEe\' = 3)') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_enum:Message', format_capn_proto_enum_comparising_mode='by_names'"
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'value Enum(\'oNe\' = 1, \'tWo\' = 2, \'threE\' = 3)') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_enum:Message', format_capn_proto_enum_comparising_mode='by_names_case_insensitive'"
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'value Enum(\'first\' = 0, \'second\' = 1, \'third\' = 2)') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_enum:Message', format_capn_proto_enum_comparising_mode='by_values'"

$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'value Enum(\'one\' = 0, \'two\' = 1, \'three\' = 2)') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_enum:Message', format_capn_proto_enum_comparising_mode='by_names'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'value Enum(\'one\' = 0, \'two\' = 1, \'tHrEe\' = 2, \'four\' = 3)') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_enum:Message', format_capn_proto_enum_comparising_mode='by_names'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'value Enum(\'one\' = 1, \'two\' = 2, \'tHrEe\' = 3)') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_enum:Message', format_capn_proto_enum_comparising_mode='by_values'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'value Enum(\'first\' = 1, \'two\' = 2, \'three\' = 3)') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_enum:Message', format_capn_proto_enum_comparising_mode='by_names_case_insensitive'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS capnp_low_cardinality"
$CLICKHOUSE_CLIENT --query="create stream capnp_low_cardinality (lc1 low_cardinality(string), lc2 low_cardinality(Nullable(string)), lc3 array(low_cardinality(Nullable(string)))) ENGINE=Memory"
$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_low_cardinality VALUES ('one', 'two', ['one', Null, 'two', Null]), ('two', Null, [Null])"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_low_cardinality FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_low_cardinality:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_low_cardinality FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_low_cardinality:Message'"
$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_low_cardinality"
$CLICKHOUSE_CLIENT --query="DROP STREAM capnp_low_cardinality"


$CLICKHOUSE_CLIENT --query="SELECT CAST(tuple(number, tuple(number + 1, tuple(number + 2))), 'tuple(b uint64, c tuple(d uint64, e tuple(f uint64)))') AS a FROM numbers(5) FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_nested_tuples:Message'" > $CAPN_PROTO_FILE
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'a_b uint64, a_c_d uint64, a_c_e_f uint64') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_nested_tuples:Message'"


$CLICKHOUSE_CLIENT --query="SELECT number AS a_b, number + 1 AS a_c_d, number + 2 AS a_c_e_f FROM numbers(5) FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_nested_tuples:Message'" > $CAPN_PROTO_FILE
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'a tuple(b uint64, c tuple(d uint64, e tuple(f uint64)))') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_nested_tuples:Message'"
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'a tuple(bb uint64, c tuple(d uint64, e tuple(f uint64)))') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_nested_tuples:Message'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'a tuple(b uint64, c tuple(d uint64, e tuple(ff uint64)))') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_nested_tuples:Message'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';


$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'string string') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_simple_types:Message'" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL';


$CLICKHOUSE_CLIENT --query="SELECT number AS uint64 FROM numbers(5) FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_simple_types:Message'" > $CAPN_PROTO_FILE
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'uint64 string') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_simple_types:Message'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'uint64 array(uint64)') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_simple_types:Message'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'uint64 Enum(\'one\' = 1)') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_simple_types:Message'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'uint64 tuple(uint64)') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_simple_types:Message'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'uint64 Nullable(uint64)') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_simple_types:Message'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT * FROM file('data.capnp', 'CapnProto', 'uint64 int32') SETTINGS format_schema='$SERVER_SCHEMADIR/02030_capnp_simple_types:Message'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';


$CLICKHOUSE_CLIENT --query="SELECT number AS a, to_string(number) as b FROM numbers(5) FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_unnamed_union:Message'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT to_nullable(to_string(number)) as nullable1 FROM numbers(5) FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_fake_nullable:Message'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="SELECT to_nullable(to_string(number)) as nullable2 FROM numbers(5) FORMAT CapnProto SETTINGS format_schema='$CLIENT_SCHEMADIR/02030_capnp_fake_nullable:Message'" 2>&1 | grep -F -q "CAPN_PROTO_BAD_CAST" && echo 'OK' || echo 'FAIL';

rm $CAPN_PROTO_FILE
rm -rf ${SCHEMADIR:?}/${SERVER_SCHEMADIR:?}
