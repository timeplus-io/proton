#!/usr/bin/env bash
# Tags: long, zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop stream if exists rmt1 sync;"
$CLICKHOUSE_CLIENT -q "drop stream if exists rmt2 sync;"

$CLICKHOUSE_CLIENT -q "create stream rmt1 (n int) engine=ReplicatedMergeTree('/test/02369/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/{database}', '1') order by n;"
$CLICKHOUSE_CLIENT -q "create stream rmt2 (n int) engine=ReplicatedMergeTree('/test/02369/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/{database}', '2') order by n;"

$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into rmt1 values (1);"
$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into rmt1 values (2);"

$CLICKHOUSE_CLIENT -q "system sync replica rmt1;"
$CLICKHOUSE_CLIENT -q "system sync replica rmt2;"
$CLICKHOUSE_CLIENT -q "system stop merges rmt2;"
$CLICKHOUSE_CLIENT -q "optimize stream rmt1 final;"

$CLICKHOUSE_CLIENT -q "select 1, *, _part from rmt1 order by n;"
$CLICKHOUSE_CLIENT -q "select 2, *, _part from rmt2 order by n;"

path=$($CLICKHOUSE_CLIENT -q "select path from system.parts where database='$CLICKHOUSE_DATABASE' and stream='rmt1' and name='all_0_1_1'")
# ensure that path is absolute before removing
$CLICKHOUSE_CLIENT -q "select throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path')" || exit
rm -rf $path

$CLICKHOUSE_CLIENT -q "select * from rmt1;" 2>/dev/null

$CLICKHOUSE_CLIENT -q "detach stream rmt1;"
$CLICKHOUSE_CLIENT -q "attach stream rmt1;"

$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into rmt1 values (3);"
$CLICKHOUSE_CLIENT -q "system start merges rmt2;"
$CLICKHOUSE_CLIENT -q "system sync replica rmt1;"
$CLICKHOUSE_CLIENT -q "optimize stream rmt1 final;"

$CLICKHOUSE_CLIENT -q "system sync replica rmt1;"
$CLICKHOUSE_CLIENT -q "system sync replica rmt2;"
$CLICKHOUSE_CLIENT -q "select 3, *, _part from rmt1 order by n;"
$CLICKHOUSE_CLIENT -q "select 4, *, _part from rmt2 order by n;"

$CLICKHOUSE_CLIENT -q "drop stream rmt1 sync;"
$CLICKHOUSE_CLIENT -q "drop stream rmt2 sync;"
