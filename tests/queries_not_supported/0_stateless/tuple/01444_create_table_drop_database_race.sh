#!/usr/bin/env bash
# Tags: race, no-parallel

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This test reproduces "Directory not empty" error in DROP DATABASE query.

function thread1()
{
    while true; do
#        ${CLICKHOUSE_CLIENT} --query="SHOW TABLES FROM test_01444"
        ${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS test_01444" 2>&1| grep -F "Code: " | grep -Fv "Code: 219"
        ${CLICKHOUSE_CLIENT} --query="CREATE DATABASE IF NOT EXISTS test_01444"
    done
}

function thread2()
{
    while true; do
        ${CLICKHOUSE_CLIENT} --query="create stream IF NOT EXISTS test_01444.t$RANDOM (x uint8) ENGINE = MergeTree ORDER BY tuple()" 2>/dev/null
    done
}

export -f thread1
export -f thread2

TIMEOUT=10

timeout $TIMEOUT bash -c thread1 &
timeout $TIMEOUT bash -c thread2 &
timeout $TIMEOUT bash -c thread2 &

wait

${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS test_01444" 2>&1| grep -F "Code: " | grep -Fv "Code: 219" || exit 0
