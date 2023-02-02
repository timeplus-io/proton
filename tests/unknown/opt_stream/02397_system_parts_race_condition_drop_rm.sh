#!/usr/bin/env bash
# Tags: race, zookeeper, no-parallel, no-backward-compatibility-check, disabled

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

set -e

# NOTE this test is copy of 00993_system_parts_race_condition_drop_zookeeper, but with extra thread7

function thread1()
{
    # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
    while true; do
        $CLICKHOUSE_CLIENT --query "SELECT * FROM system.parts FORMAT Null";
    done
}

function thread2()
{
    while true; do
        REPLICA=$(($RANDOM % 10))
        $CLICKHOUSE_CLIENT -n --query "ALTER STREAM alter_table_$REPLICA ADD COLUMN h string '0'; ALTER STREAM alter_table_$REPLICA MODIFY COLUMN h uint64; ALTER STREAM alter_table_$REPLICA DROP COLUMN h;"; 
    done
}

function thread3()
{
    while true; do
        REPLICA=$(($RANDOM % 10))
        $CLICKHOUSE_CLIENT -q "INSERT INTO alter_table_$REPLICA SELECT rand(1), rand(2), 1 / rand(3), to_string(rand(4)), [rand(5), rand(6)], rand(7) % 2 ? NULL : generateUUIDv4(), (rand(8), rand(9)) FROM numbers(100000)"; 
    done
}

function thread4()
{
    while true; do
        REPLICA=$(($RANDOM % 10))
        $CLICKHOUSE_CLIENT -q "OPTIMIZE STREAM alter_table_$REPLICA FINAL"; 
        sleep 0.$RANDOM;
    done
}

function thread5()
{
    while true; do
        REPLICA=$(($RANDOM % 10))
        $CLICKHOUSE_CLIENT -q "ALTER STREAM alter_table_$REPLICA DELETE WHERE cityHash64(a,b,c,d,e,g) % 1048576 < 524288";
        sleep 0.$RANDOM;
    done
}

function thread6()
{
    while true; do
        REPLICA=$(($RANDOM % 10))
        $CLICKHOUSE_CLIENT -n -q "DROP STREAM IF EXISTS alter_table_$REPLICA;
            CREATE STREAM alter_table_$REPLICA (a uint8, b int16, c float32, d string, e array(uint8), f nullable(uuid), g Tuple(uint8, uint16)) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r_$REPLICA') ORDER BY a PARTITION BY b % 10 SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0;";
        sleep 0.$RANDOM;
    done
}

function thread7()
{
    while true; do
        path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database='$CLICKHOUSE_DATABASE' AND stream LIKE 'alter_table_%' ORDER BY rand() LIMIT 1")
        if [ -z "$path" ]; then continue; fi
        # ensure that path is absolute before removing
        $CLICKHOUSE_CLIENT -q "select throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path') format Null" || exit
        rm -rf $path 2> /dev/null
        sleep 0.$RANDOM;
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;
export -f thread3;
export -f thread4;
export -f thread5;
export -f thread6;
export -f thread7;

TIMEOUT=15

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &
timeout $TIMEOUT bash -c thread6 2>&1 | grep "was not completely removed from ZooKeeper" &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &
timeout $TIMEOUT bash -c thread6 2>&1 | grep "was not completely removed from ZooKeeper" &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &
timeout $TIMEOUT bash -c thread6 2>&1 | grep "was not completely removed from ZooKeeper" &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &
timeout $TIMEOUT bash -c thread6 2>&1 | grep "was not completely removed from ZooKeeper" &

timeout $TIMEOUT bash -c thread7 &

wait

check_replication_consistency "alter_table_" "count(), sum(a), sum(b), round(sum(c))"

for i in {0..9}; do
    $CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS alter_table_$i" 2>&1 | grep "was not completely removed from ZooKeeper" &
done

wait
