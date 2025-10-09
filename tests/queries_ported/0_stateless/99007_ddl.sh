#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# Generate the explicit UUID
explicit_uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid()")

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS default.test_99007ddl"

# Create the stream with the explicit UUID
$CLICKHOUSE_CLIENT -q "CREATE STREAM default.test_99007ddl UUID '$explicit_uuid' (id int)"

# Extract the UUID from the stream creation statement
extracted_uuid=$($CLICKHOUSE_CLIENT -q "SHOW CREATE default.test_99007ddl FORMAT CSV SETTINGS show_uuid=true" | grep "UUID" | awk -F"'" '{print $2}')

# Fetch the UUID using curl
stream_name="test_99007ddl"
curl_response=$(${CLICKHOUSE_CURL} -X "GET" "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/ddl/streams/default/${stream_name}")
curl_uuid=$(echo $curl_response | sed -n 's/.*"uuid":"\([^"]*\)".*/\1/p')


# Verify if the extracted UUID matches the expected UUID
if [ "$explicit_uuid" = "$extracted_uuid" ] && [ "$explicit_uuid" = "$curl_uuid" ]; then
  echo "UUID matches"
else
  echo "UUID does not match."
  exit 1
fi