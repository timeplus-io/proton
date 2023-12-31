#!/usr/bin/env bash
# Tags: long

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "DROP STREAM IF EXISTS concurrent_alter_column" | ${CLICKHOUSE_CLIENT}
echo "create stream concurrent_alter_column (ts DATETIME) ENGINE = MergeTree PARTITION BY to_start_of_day(ts) ORDER BY tuple()" | ${CLICKHOUSE_CLIENT}

function thread1()
{
    while true; do
        for i in {1..500}; do echo "ALTER STREAM concurrent_alter_column ADD COLUMN c$i DOUBLE;"; done | ${CLICKHOUSE_CLIENT} -n --query_id=alter_00816_1
    done
}

function thread2()
{
    while true; do
        echo "ALTER STREAM concurrent_alter_column ADD COLUMN d DOUBLE" | ${CLICKHOUSE_CLIENT} --query_id=alter_00816_2;
        sleep "$(echo 0.0$RANDOM)";
        echo "ALTER STREAM concurrent_alter_column DROP COLUMN d" | ${CLICKHOUSE_CLIENT} --query_id=alter_00816_2;
    done
}

function thread3()
{
    while true; do
        echo "ALTER STREAM concurrent_alter_column ADD COLUMN e DOUBLE" | ${CLICKHOUSE_CLIENT} --query_id=alter_00816_3;
        sleep "$(echo 0.0$RANDOM)";
        echo "ALTER STREAM concurrent_alter_column DROP COLUMN e" | ${CLICKHOUSE_CLIENT} --query_id=alter_00816_3;
    done
}

function thread4()
{
    while true; do
        echo "ALTER STREAM concurrent_alter_column ADD COLUMN f DOUBLE" | ${CLICKHOUSE_CLIENT} --query_id=alter_00816_4;
        sleep "$(echo 0.0$RANDOM)";
        echo "ALTER STREAM concurrent_alter_column DROP COLUMN f" | ${CLICKHOUSE_CLIENT} --query_id=alter_00816_4;
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;
export -f thread3;
export -f thread4;

TIMEOUT=30

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &

wait

echo "DROP STREAM concurrent_alter_column NO DELAY" | ${CLICKHOUSE_CLIENT}   # NO DELAY has effect only for Atomic database

# Wait for alters and check for deadlocks (in case of deadlock this loop will not finish)
while true; do
    echo "SELECT * FROM system.processes WHERE query_id LIKE 'alter\\_00816\\_%'" | ${CLICKHOUSE_CLIENT} | grep -q -F 'alter' || break
    sleep 1;
done

echo 'did not crash'
