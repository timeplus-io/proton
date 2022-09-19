#!/usr/bin/env bash
# Tags: race

# This test is disabled because it triggers internal assert in Thread Sanitizer.
# Thread Sanitizer does not support for more than 64 mutexes to be locked in a single thread.
# https://github.com/google/sanitizers/issues/950

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS alter_table"
$CLICKHOUSE_CLIENT -q "create stream alter_table (a uint8, b Int16, c Float32, d string, e array(uint8), f Nullable(UUID), g tuple(uint8, uint16)) ENGINE = MergeTree ORDER BY a PARTITION BY b % 10 SETTINGS old_parts_lifetime = 1"

function thread1()
{
    # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
    while true; do $CLICKHOUSE_CLIENT --query "SELECT * FROM system.parts FORMAT Null"; done
}

function thread2()
{
    while true; do $CLICKHOUSE_CLIENT -n --query "ALTER STREAM alter_table ADD COLUMN h string '0'; ALTER STREAM alter_table MODIFY COLUMN h uint64; ALTER STREAM alter_table DROP COLUMN h;"; done
}

function thread3()
{
    while true; do $CLICKHOUSE_CLIENT -q "INSERT INTO alter_table SELECT rand(1), rand(2), 1 / rand(3), to_string(rand(4)), [rand(5), rand(6)], rand(7) % 2 ? NULL : generateUUIDv4(), (rand(8), rand(9)) FROM numbers(100000)"; done
}

function thread4()
{
    while true; do $CLICKHOUSE_CLIENT -q "OPTIMIZE STREAM alter_table FINAL"; done
}

function thread5()
{
    while true; do $CLICKHOUSE_CLIENT -q "ALTER STREAM alter_table DELETE WHERE rand() % 2 = 1"; done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;
export -f thread3;
export -f thread4;
export -f thread5;

TIMEOUT=30

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &


wait

$CLICKHOUSE_CLIENT -q "DROP STREAM alter_table"
