#!/usr/bin/env bash
# Tags: race, zookeeper, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

set -e

$CLICKHOUSE_CLIENT -n -q "
    DROP STREAM IF EXISTS alter_table0;
    DROP STREAM IF EXISTS alter_table1;

    create stream alter_table0 (a uint8, b int16, c Float32, d string, e array(uint8), f Nullable(UUID), g tuple(uint8, uint16)) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r1') ORDER BY a PARTITION BY b % 10 SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0;
    create stream alter_table1 (a uint8, b int16, c Float32, d string, e array(uint8), f Nullable(UUID), g tuple(uint8, uint16)) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r2') ORDER BY a PARTITION BY b % 10 SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0
"

function thread1()
{
    # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
    while true; do $CLICKHOUSE_CLIENT --query "SELECT * FROM system.parts FORMAT Null"; done
}

function thread2()
{
    while true; do $CLICKHOUSE_CLIENT -n --query "ALTER STREAM alter_table0 ADD COLUMN h string DEFAULT '0'; ALTER STREAM alter_table0 MODIFY COLUMN h uint64; ALTER STREAM alter_table0 DROP COLUMN h;"; done
}

function thread3()
{
    while true; do $CLICKHOUSE_CLIENT -q "INSERT INTO alter_table0 SELECT rand(1), rand(2), 1 / rand(3), to_string(rand(4)), [rand(5), rand(6)], rand(7) % 2 ? NULL : generateUUIDv4(), (rand(8), rand(9)) FROM numbers(100000)"; done
}

function thread4()
{
    while true; do $CLICKHOUSE_CLIENT -q "OPTIMIZE STREAM alter_table0 FINAL"; done
}

function thread5()
{
    while true; do $CLICKHOUSE_CLIENT -q "ALTER STREAM alter_table0 DELETE WHERE cityHash64(a,b,c,d,e,g) % 1048576 < 524288"; done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;
export -f thread3;
export -f thread4;
export -f thread5;

TIMEOUT=10

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
check_replication_consistency "alter_table" "count(), sum(a), sum(b), round(sum(c))"

$CLICKHOUSE_CLIENT -n -q "DROP STREAM alter_table0;" 2> >(grep -F -v 'is already started to be removing by another replica right now') &
$CLICKHOUSE_CLIENT -n -q "DROP STREAM alter_table1;" 2> >(grep -F -v 'is already started to be removing by another replica right now') &

wait
