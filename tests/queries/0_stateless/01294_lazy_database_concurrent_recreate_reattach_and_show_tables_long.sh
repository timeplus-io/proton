#!/usr/bin/env bash
# Tags: long, no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export CURR_DATABASE="test_lazy_01294_concurrent_${CLICKHOUSE_DATABASE}"


function recreate_lazy_func1()
{
    $CLICKHOUSE_CLIENT -q "
        create stream $CURR_DATABASE.log (a uint64, b uint64)  ;
    ";

    while true; do
        $CLICKHOUSE_CLIENT -q "
            DETACH TABLE $CURR_DATABASE.log;
        ";

        $CLICKHOUSE_CLIENT -q "
            ATTACH TABLE $CURR_DATABASE.log;
        ";
        done
}

function recreate_lazy_func2()
{
    while true; do
        $CLICKHOUSE_CLIENT -q "
            create stream $CURR_DATABASE.tlog (a uint64, b uint64) ;
        ";

        $CLICKHOUSE_CLIENT -q "
            DROP STREAM $CURR_DATABASE.tlog;
        ";
        done
}

function recreate_lazy_func3()
{
    $CLICKHOUSE_CLIENT -q "
        create stream $CURR_DATABASE.slog (a uint64, b uint64) ENGINE = StripeLog;
    ";

    while true; do
        $CLICKHOUSE_CLIENT -q "
            ATTACH TABLE $CURR_DATABASE.slog;
        ";

        $CLICKHOUSE_CLIENT -q "
            DETACH TABLE $CURR_DATABASE.slog;
        ";
        done
}

function recreate_lazy_func4()
{
    while true; do
        $CLICKHOUSE_CLIENT -q "
            create stream $CURR_DATABASE.tlog2 (a uint64, b uint64) ;
        ";

        $CLICKHOUSE_CLIENT -q "
            DROP STREAM $CURR_DATABASE.tlog2;
        ";
        done
}

function test_func()
{
    while true; do
        for table in log tlog slog tlog2; do
            $CLICKHOUSE_CLIENT -q "SYSTEM STOP TTL MERGES $CURR_DATABASE.$table" >& /dev/null
        done
    done
}


export -f recreate_lazy_func1;
export -f recreate_lazy_func2;
export -f recreate_lazy_func3;
export -f recreate_lazy_func4;
export -f test_func;


${CLICKHOUSE_CLIENT} -n -q "
    DROP DATABASE IF EXISTS $CURR_DATABASE;
    CREATE DATABASE $CURR_DATABASE ENGINE = Lazy(1);
"


TIMEOUT=30

timeout $TIMEOUT bash -c recreate_lazy_func1 2> /dev/null &
timeout $TIMEOUT bash -c recreate_lazy_func2 2> /dev/null &
timeout $TIMEOUT bash -c recreate_lazy_func3 2> /dev/null &
timeout $TIMEOUT bash -c recreate_lazy_func4 2> /dev/null &
timeout $TIMEOUT bash -c test_func 2> /dev/null &

wait
sleep 1

for table in log tlog slog tlog2; do
    $CLICKHOUSE_CLIENT -q "SYSTEM STOP TTL MERGES $CURR_DATABASE.$table" >& /dev/null
  ${CLICKHOUSE_CLIENT} -q "ATTACH TABLE $CURR_DATABASE.$table;" 2>/dev/null
done

${CLICKHOUSE_CLIENT} -q "DROP DATABASE $CURR_DATABASE"

echo "Test OK"

