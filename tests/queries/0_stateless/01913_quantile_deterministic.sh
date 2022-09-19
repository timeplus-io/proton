#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS d"
${CLICKHOUSE_CLIENT} --query "create stream d (oid uint64) ENGINE = MergeTree ORDER BY oid"
${CLICKHOUSE_CLIENT} --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 --max_block_size 8192 --query "insert into d select * from numbers(1000000)"

# In previous ClickHouse versions there was a mistake that makes quantileDeterministic functions not really deterministic (in edge cases).

for _ in {1..20};
do
    ${CLICKHOUSE_CLIENT} --query "SELECT medianDeterministic(oid, oid) FROM d"
done

${CLICKHOUSE_CLIENT} --query "DROP STREAM d"
