#!/usr/bin/env bash
# Tags: deadlock

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP STREAM IF EXISTS a"
$CLICKHOUSE_CLIENT --query "DROP STREAM IF EXISTS b"

$CLICKHOUSE_CLIENT --query "create stream a (x uint8) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT --query "create stream b (x uint8) ENGINE = MergeTree ORDER BY tuple()"


function thread1()
{
    while true; do
        # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
        seq 1 100 | awk '{ print "SELECT x FROM a WHERE x IN (SELECT to_uint8(count()) FROM system.tables);" }' | $CLICKHOUSE_CLIENT -n
    done
}

function thread2()
{
    while true; do
        # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
        seq 1 100 | awk '{ print "SELECT x FROM b WHERE x IN (SELECT to_uint8(count()) FROM system.tables);" }' | $CLICKHOUSE_CLIENT -n
    done
}

function thread3()
{
    while true; do 
        $CLICKHOUSE_CLIENT --query "ALTER STREAM a MODIFY COLUMN x Nullable(uint8)"
        $CLICKHOUSE_CLIENT --query "ALTER STREAM a MODIFY COLUMN x uint8"
    done
}

function thread4()
{
    while true; do 
        $CLICKHOUSE_CLIENT --query "ALTER STREAM b MODIFY COLUMN x Nullable(uint8)"
        $CLICKHOUSE_CLIENT --query "ALTER STREAM b MODIFY COLUMN x uint8"
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

$CLICKHOUSE_CLIENT --query "DROP STREAM a"
$CLICKHOUSE_CLIENT --query "DROP STREAM b"
