#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --optimize_move_to_prewhere=1 --convert_query_to_cnf=0 --optimize_read_in_order=1"

$CLICKHOUSE_CLIENT -q "drop stream if exists test_index"
$CLICKHOUSE_CLIENT -q "drop stream if exists idx"

$CLICKHOUSE_CLIENT -q "create stream test_index (x uint32, y uint32, z uint32, t uint32, index t_minmax t % 20 TYPE minmax GRANULARITY 2, index t_set t % 19 type set(4) granularity 2) engine = MergeTree order by (x, y) partition by (y, bit_and(z, 3), int_div(t, 15)) settings index_granularity = 2, min_bytes_for_wide_part = 0"
$CLICKHOUSE_CLIENT -q "insert into test_index select number, number > 3 ? 3 : number, number = 1 ? 1 : 0, number from numbers(20)"

$CLICKHOUSE_CLIENT -q "
    explain indexes = 1 select *, _part from test_index where t % 19 = 16 and y > 0 and bit_and(z, 3) != 1 and x > 10 and t % 20 > 14;
    " | grep -A 100 "ReadFromMergeTree" # | grep -v "Description"

echo "-----------------"

$CLICKHOUSE_CLIENT -q "
    explain indexes = 1, json = 1 select *, _part from test_index where t % 19 = 16 and y > 0 and bit_and(z, 3) != 1 and x > 10 and t % 20 > 14 format TSVRaw;
    " | grep -A 100 "ReadFromMergeTree" # | grep -v "Description"

echo "-----------------"

$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select x from test_index where x > 15 order by x;
    " | grep -A 100 "ReadFromMergeTree"

echo "-----------------"

$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select x from test_index where x > 15 order by x desc;
    " | grep -A 100 "ReadFromMergeTree"

$CLICKHOUSE_CLIENT -q "CREATE STREAM idx (x uint32, y uint32, z uint32) ENGINE = MergeTree ORDER BY (x, x + y) settings min_bytes_for_wide_part = 0"
$CLICKHOUSE_CLIENT -q "insert into idx select number, number, number from numbers(10)"

$CLICKHOUSE_CLIENT -q "
    explain indexes = 1 select z from idx where not(x + y + 1 > 2 and x not in (4, 5))
    " | grep -A 100 "ReadFromMergeTree"

$CLICKHOUSE_CLIENT -q "drop stream if exists test_index"
$CLICKHOUSE_CLIENT -q "drop stream if exists idx"
