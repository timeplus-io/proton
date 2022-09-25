#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS alter_table"

${CLICKHOUSE_CLIENT} --query "create stream alter_table (key uint64, value string) ENGINE MergeTree ORDER BY key"

# we don't need mutations and merges
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES alter_table"

${CLICKHOUSE_CLIENT} --query "INSERT INTO alter_table SELECT number, to_string(number) FROM numbers(10000)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO alter_table SELECT number, to_string(number) FROM numbers(10000, 10000)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO alter_table SELECT number, to_string(number) FROM numbers(20000, 10000)"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM alter_table WHERE value == '733'"

${CLICKHOUSE_CLIENT} --query "ALTER STREAM alter_table MODIFY COLUMN value low_cardinality(string)" &

# waiting until schema will change (but not data)
show_query="SHOW create stream alter_table"
create_query=""
while [[ "$create_query" != *"low_cardinality"* ]]
do
    sleep 0.1
    create_query=$($CLICKHOUSE_CLIENT --query "$show_query")
done

# checking type is LowCardinalty
${CLICKHOUSE_CLIENT} --query "SHOW create stream alter_table"

# checking no mutations happened
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.parts where table='alter_table' and active and database='${CLICKHOUSE_DATABASE}' ORDER BY name"

# checking that conversions applied "on fly" works
${CLICKHOUSE_CLIENT} --query "SELECT * FROM alter_table PREWHERE key > 700 WHERE value = '701'"

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS alter_table"
