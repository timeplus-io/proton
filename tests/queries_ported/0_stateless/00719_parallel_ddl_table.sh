#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS parallel_ddl"

function query()
{
    for _ in {1..100}; do
        ${CLICKHOUSE_CLIENT} --query "CREATE STREAM IF NOT EXISTS parallel_ddl(a int) ENGINE = Memory"
        ${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS parallel_ddl"
    done
}

for _ in {1..2}; do
    query &
done

wait

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS parallel_ddl"
