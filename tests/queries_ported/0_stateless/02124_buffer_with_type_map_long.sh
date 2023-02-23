#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for incorrect mutation of map() column, see [1].
#
#   [1]: https://github.com/ClickHouse/ClickHouse/issues/30546

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS t_buffer_map"
$CLICKHOUSE_CLIENT -q "CREATE STREAM t_buffer_map(m1 map(string, uint64), m2 map(string, string)) ENGINE = Buffer('', '', 1, 1, 1, 1000000000000, 1000000000000, 1000000000000, 1000000000000)"

# --continue_on_errors -- to ignore possible MEMORY_LIMIT_EXCEEDED errors
$CLICKHOUSE_BENCHMARK --randomize --timelimit 10 --continue_on_errors --concurrency 10 >& /dev/null <<EOL
INSERT INTO t_buffer_map SELECT (range(10), range(10)), (range(10), range(10)) from numbers(100)
SELECT * FROM t_buffer_map
EOL

echo "OK"
$CLICKHOUSE_CLIENT -q "DROP STREAM t_buffer_map"
