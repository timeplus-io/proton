#!/usr/bin/env bash
# Tags: no-parallel
# Test: protobuf schema registry import resolution (issue #1076)
#
# Verifies that protobuf schemas with import statements
# (e.g. import "google/protobuf/timestamp.proto") are correctly
# resolved when using format_schema with CREATE FORMAT SCHEMA.
#
# This exercises the same DescriptorPool + resolveReferences()
# code path used by kafka_schema_registry_url.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCHEMADIR=$CURDIR/format_schemas

# Cleanup
$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS test_proto_import_99177;"
$CLICKHOUSE_CLIENT -q "DROP FORMAT SCHEMA IF EXISTS test_import_schema_99177 TYPE Protobuf;"

# Step 1: Create a format schema with an import statement
$CLICKHOUSE_CLIENT -q "
CREATE FORMAT SCHEMA test_import_schema_99177 AS \$\$
syntax = \"proto3\";
import \"google/protobuf/timestamp.proto\";

message TestEvent {
  int64 id = 1;
  string name = 2;
  google.protobuf.Timestamp created_at = 3;
}
\$\$ TYPE Protobuf
"

# Step 2: Create a stream using the schema
$CLICKHOUSE_CLIENT -q "
CREATE STREAM test_proto_import_99177 (
  id int64,
  name string,
  created_at tuple(seconds int64, nanos int32)
) ENGINE = Memory;
"

# Step 3: Insert data using the protobuf schema
# The import must resolve for the INSERT to succeed
$CLICKHOUSE_CLIENT -q "INSERT INTO test_proto_import_99177 (id, name, created_at) VALUES (1, 'event-a', (1735100000, 123456789)), (2, 'event-b', (1735100001, 0)), (3, 'event-c', (1735100002, 999999999));"

# Step 4: Read back and verify
$CLICKHOUSE_CLIENT -q "SELECT id, name, created_at FROM test_proto_import_99177 ORDER BY id;"

# Cleanup
$CLICKHOUSE_CLIENT -q "DROP STREAM test_proto_import_99177;"
$CLICKHOUSE_CLIENT -q "DROP FORMAT SCHEMA IF EXISTS test_import_schema_99177 TYPE Protobuf;"
