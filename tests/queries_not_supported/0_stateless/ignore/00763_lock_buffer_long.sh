#!/usr/bin/env bash
# Tags: long

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS mt_00763_2"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS buffer_00763_2"

${CLICKHOUSE_CLIENT} --query="create stream buffer_00763_2 (s string) ENGINE = Buffer('$CLICKHOUSE_DATABASE', mt_00763_2, 1, 1, 1, 1, 1, 1, 1)"


function thread1()
{
    seq 1 500 | sed -r -e 's/.+/DROP STREAM IF EXISTS mt_00763_2; create stream mt_00763_2 (s string) ENGINE = MergeTree ORDER BY s; INSERT INTO mt_00763_2 SELECT to_string(number) FROM numbers(10);/' | ${CLICKHOUSE_CLIENT} --multiquery --ignore-error ||:
}

function thread2()
{
    seq 1 1000 | sed -r -e 's/.+/SELECT count() FROM buffer_00763_2;/' | ${CLICKHOUSE_CLIENT} --multiquery --server_logs_file='/dev/null' --ignore-error 2>&1 | grep -vP '^0$|^10$|^Received exception|^Code: 60|^Code: 218|^Code: 473' | grep -v '(query: '
}

thread1 &
thread2 &

wait

${CLICKHOUSE_CLIENT} --query="DROP STREAM mt_00763_2"
${CLICKHOUSE_CLIENT} --query="DROP STREAM buffer_00763_2"
