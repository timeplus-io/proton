#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Create and update format schemas
$CLICKHOUSE_CLIENT -m -n -q "
CREATE FORMAT SCHEMA stateless_99064_proto AS '
syntax = \"proto3\";

message SearchRequest {
  string query = 1;
}
' TYPE Protobuf;

CREATE OR REPLACE FORMAT SCHEMA stateless_99064_proto AS '
syntax = \"proto3\";

message SearchRequest {
  string query = 2;
}
' TYPE Protobuf;

CREATE OR REPLACE FORMAT SCHEMA stateless_99064_proto AS '
syntax = \"proto3\";

message SearchRequest {
  string query = 3;
}
' TYPE Protobuf;

CREATE FORMAT SCHEMA stateless_99064_avro AS '
{
  \"namespace\": \"example.avro\",
  \"type\": \"record\",
  \"name\": \"User\",
  \"fields\": [
    {\"name\": \"name\", \"type\": \"string\"}
  ]
}
' TYPE Avro;
"

# Since the `last_modified` and `created` fields are dynamic, skip them
$CLICKHOUSE_CLIENT -q 'show format schemas format Vertical settings verbose=true' | grep -Ev 'last_modified:|created:'

# Since the `last_modified` and `created` fields are dynamic, skip them
$CLICKHOUSE_CLIENT -q 'show create format schema stateless_99064_proto format Vertical settings show_multi_versions=true' | grep -Ev 'last_modified:|created:'
