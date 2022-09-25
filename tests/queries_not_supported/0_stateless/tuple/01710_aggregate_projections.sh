#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "create stream test_agg_proj (x int32, y int32, PROJECTION x_plus_y (SELECT sum(x - y), arg_max(x, y) group by x + y)) ENGINE = MergeTree ORDER BY tuple() settings index_granularity = 1"
$CLICKHOUSE_CLIENT -q "insert into test_agg_proj select int_div(number, 2), -int_div(number,3) - 1 from numbers(100)"

$CLICKHOUSE_CLIENT -q "select x + y, sum(x - y) as s from test_agg_proj group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1"
$CLICKHOUSE_CLIENT -q "select x + y, sum(x - y) as s from test_agg_proj group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1 format JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -q "select (x + y) * 2, sum(x - y) * 2 as s from test_agg_proj group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1"
$CLICKHOUSE_CLIENT -q "select (x + y) * 2, sum(x - y) * 2 as s from test_agg_proj group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1 format JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -q "select int_div(x + y, 2), int_div(x + y, 3), sum(x - y) as s from test_agg_proj group by int_div(x + y, 2), int_div(x + y, 3) order by s desc limit 5 settings allow_experimental_projection_optimization=1"
$CLICKHOUSE_CLIENT -q "select int_div(x + y, 2), int_div(x + y, 3), sum(x - y) as s from test_agg_proj group by int_div(x + y, 2), int_div(x + y, 3) order by s desc limit 5 settings allow_experimental_projection_optimization=1 format JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -q "select x + y + 1, arg_max(x, y) * sum(x - y) as s from test_agg_proj group by x + y + 1 order by s desc limit 5 settings allow_experimental_projection_optimization=1"
$CLICKHOUSE_CLIENT -q "select x + y + 1, arg_max(x, y) * sum(x - y) as s from test_agg_proj group by x + y + 1 order by s desc limit 5 settings allow_experimental_projection_optimization=1 format JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -q "select x + y + 1, arg_max(y, x), sum(x - y) as s from test_agg_proj group by x + y + 1 order by s desc limit 5 settings allow_experimental_projection_optimization=1"
$CLICKHOUSE_CLIENT -q "select x + y + 1, arg_max(y, x), sum(x - y) as s from test_agg_proj group by x + y + 1 order by s desc limit 5 settings allow_experimental_projection_optimization=1 format JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -q "select x + y, sum(x - y) as s from test_agg_proj prewhere (x + y) % 2 = 1 group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1"
$CLICKHOUSE_CLIENT -q "select x + y, sum(x - y) as s from test_agg_proj prewhere (x + y) % 2 = 1 group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1 format JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -q "drop stream test_agg_proj"
