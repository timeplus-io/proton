#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

REST_ENDPOINT="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/dependencies"

$CLICKHOUSE_CLIENT --multiquery <<EOF
CREATE DATABASE IF NOT EXISTS db_99097;
CREATE STREAM IF NOT EXISTS db_99097.stream1(id int);
CREATE STREAM IF NOT EXISTS db_99097.stream2(id int);
CREATE STREAM IF NOT EXISTS db_99097.stream3(id int);
CREATE MATERIALIZED VIEW IF NOT EXISTS db_99097.mv1 AS SELECT * FROM system.introspection_state_log;
CREATE OR REPLACE TASK db_99097.task1 SCHEDULE 10d INTO db_99097.stream2 AS SELECT id FROM db_99097.stream1 a JOIN db_99097.stream3 b ON a.id=b.id;
EOF

echo "Check count"
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097" | jq '.data.dependencies | length'
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097" | jq '.data.data_flow | length'

echo "Check mv1 dependencies"
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097" | jq '.data.dependencies[] | select(.from == "db_99097.mv1")'
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097" | jq '.data.dependencies[] | select(.to == "db_99097.mv1")'
echo "Check mv1 data flows"
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097" | jq '.data.data_flow[] | select(.from == "db_99097.mv1")'
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097" | jq '.data.data_flow[] | select(.to == "db_99097.mv1")'

echo "Check task1 dependencies"
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097" | jq '.data.dependencies[] | select(.from == "db_99097.task1") | length'
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097" | jq '.data.dependencies[] | select(.to == "db_99097.task1")'
echo "Check task1 data flows"
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097" | jq '.data.data_flow[] | select(.from == "db_99097.task1")'
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097" | jq '.data.data_flow[] | select(.to == "db_99097.task1")'

echo "Get mv1 dependencies"
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097/mv1" | jq '.data.dependencies | length'
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097/mv1" | jq '.data.data_flow[] | select(.from == "db_99097.mv1")'
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097/mv1" | jq '.data.data_flow[] | select(.to == "db_99097.mv1")'

echo "Get task1 dependencies"
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097/task1" | jq '.data.dependencies | length'
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/db_99097/task1" | jq '.data.data_flow[] | select(.from == "db_99097.task1")'


$CLICKHOUSE_CLIENT -q "DROP DATABASE db_99097 CASCADE"
