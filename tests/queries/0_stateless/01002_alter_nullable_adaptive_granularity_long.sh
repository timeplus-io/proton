#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "drop stream IF EXISTS test";
$CLICKHOUSE_CLIENT --query "create stream test (x uint8, s string MATERIALIZED to_string(rand64())) ENGINE = MergeTree ORDER BY s";

function thread1()
{
    while true; do 
        $CLICKHOUSE_CLIENT --query "INSERT INTO test SELECT rand() FROM numbers(1000)";
    done
}

function thread2()
{
    while true; do
        $CLICKHOUSE_CLIENT -n --query "ALTER STREAM test MODIFY COLUMN x Nullable(uint8);";
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT -n --query "ALTER STREAM test MODIFY COLUMN x uint8;";
        sleep 0.0$RANDOM
    done
}

function thread3()
{
    while true; do
        $CLICKHOUSE_CLIENT -n --query "SELECT count() FROM test FORMAT Null";
    done
}

function thread4()
{
    while true; do
        $CLICKHOUSE_CLIENT -n --query "OPTIMIZE STREAM test FINAL";
        sleep 0.1$RANDOM
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;
export -f thread3;
export -f thread4;

TIMEOUT=10

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "drop stream test"
