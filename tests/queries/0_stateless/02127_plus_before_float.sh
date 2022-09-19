#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "select to_float32('+42.42'), toFloat64('+42.42')"
$CLICKHOUSE_CLIENT -q "drop stream if exists test_02127"
$CLICKHOUSE_CLIENT -q "create stream test_02127 (x Float32, y float64) engine=Memory()"

for escaping_rule in Quoted JSON Escaped CSV Raw
do
echo -e "+42.42\t+42.42" | $CLICKHOUSE_CLIENT -q "insert into test_02127 format CustomSeparated settings format_custom_escaping_rule='$escaping_rule'"
done


$CLICKHOUSE_CLIENT -q "select * from test_02127"
$CLICKHOUSE_CLIENT -q "drop stream test_02127"
