#!/usr/bin/env bash
# Tags: race

# Test fix for issue #5066

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS alter_table"
$CLICKHOUSE_CLIENT -q "create stream alter_table (a uint8, b int16, c Float32, d string, e array(uint8), f Nullable(UUID), g tuple(uint8, uint16)) ENGINE = MergeTree ORDER BY a"

function thread1()
{
    # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
    while true; do $CLICKHOUSE_CLIENT --query "SELECT name FROM system.columns UNION ALL SELECT name FROM system.columns FORMAT Null"; done
}

function thread2()
{
    while true; do $CLICKHOUSE_CLIENT -n --query "ALTER STREAM alter_table ADD COLUMN h string; ALTER STREAM alter_table MODIFY COLUMN h uint64; ALTER STREAM alter_table DROP COLUMN h;"; done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;

timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP STREAM alter_table"
