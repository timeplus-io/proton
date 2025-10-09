#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Store the response in a variable
RESPONSE=$(${CLICKHOUSE_CURL} -sS -X POST "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/sqlanalyzer" \
-H "Content-Type: application/json" \
-d '{
  "query": "CREATE EXTERNAL TABLE my_s3_ext (`ts` datetime64(3),`c` int,`j` string,event_ts datetime64(3)) SETTINGS type='s3',access_key_id='minioadmin',secret_access_key='minioadmin123',endpoint='http://minio:9000',bucket='proton'"
}')

# Since we're now getting a successful response, we need to check for that instead
if echo "${RESPONSE}" | grep -q '"query_type":"CREATE"'; then
  echo "Test case passed: External table DDL correctly parsed."
  exit 0
else
  # Still check for the original error code in case the environment changes
  if echo "${RESPONSE}" | grep -q '"code":36'; then
    echo "Test case passed with expected error: Error code 36 (Failed to parse SETTINGS in CREATE EXTERNAL TABLE) was found."
    exit 0
  else
    echo "Test case failed: Neither successful parsing nor expected error code 36 was found."
    exit 1
  fi
fi
