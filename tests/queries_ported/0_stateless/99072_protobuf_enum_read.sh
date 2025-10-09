#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCHEMADIR=$CURDIR/format_schemas
DATADIR=$CURDIR/data_proto

# Run the client.
$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS test_protobuf_enum_read_99072;"

$CLICKHOUSE_CLIENT -m -q "
CREATE STREAM test_protobuf_enum_read_99072 (
  level_one string,
  level_two int,
  level_three enum('A' = 0, 'B' = 1, 'C' = 2)
) Engine = Memory;
"

# ProtobufSingle handles one record per time, thus 3 records are separated into 3 files.
$CLICKHOUSE_CLIENT -q "INSERT INTO test_protobuf_enum_read_99072(level_one, level_two, level_three) SETTINGS format_schema='$SCHEMADIR/99072_test_enum:TestEnum' FORMAT ProtobufSingle" < $DATADIR/99072_protobuf_enum_1
$CLICKHOUSE_CLIENT -q "INSERT INTO test_protobuf_enum_read_99072(level_one, level_two, level_three) SETTINGS format_schema='$SCHEMADIR/99072_test_enum:TestEnum' FORMAT ProtobufSingle" < $DATADIR/99072_protobuf_enum_2
$CLICKHOUSE_CLIENT -q "INSERT INTO test_protobuf_enum_read_99072(level_one, level_two, level_three) SETTINGS format_schema='$SCHEMADIR/99072_test_enum:TestEnum' FORMAT ProtobufSingle" < $DATADIR/99072_protobuf_enum_3

# Check the output in the protobuf format
$CLICKHOUSE_CLIENT -q "SELECT level_one, level_two, level_three FROM test_protobuf_enum_read_99072 ORDER BY level_two"

# Cleanup
$CLICKHOUSE_CLIENT -q "DROP STREAM test_protobuf_enum_read_99072"
