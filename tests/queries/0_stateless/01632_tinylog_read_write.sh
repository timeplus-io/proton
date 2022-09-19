#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --multiquery --query "DROP STREAM IF EXISTS test; create stream IF NOT EXISTS test (x uint64, s array(Nullable(string))) ;"

function thread_select {
    while true; do
        $CLICKHOUSE_CLIENT --query "SELECT * FROM test FORMAT Null"
        sleep 0.0$RANDOM
    done
}

function thread_insert {
    while true; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO test VALUES (1, ['Hello'])"
        sleep 0.0$RANDOM
    done
}

export -f thread_select
export -f thread_insert


# Do randomized queries and expect nothing extraordinary happens.

timeout 10 bash -c 'thread_select' &
timeout 10 bash -c 'thread_select' &
timeout 10 bash -c 'thread_select' &
timeout 10 bash -c 'thread_select' &

timeout 10 bash -c 'thread_insert' &
timeout 10 bash -c 'thread_insert' &
timeout 10 bash -c 'thread_insert' &
timeout 10 bash -c 'thread_insert' &

wait
echo "Done"

$CLICKHOUSE_CLIENT --multiquery --query "DROP STREAM IF EXISTS test;"
