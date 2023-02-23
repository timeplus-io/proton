#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_LOCAL -q "select to_uint32(number) as x from numbers(10) format JSONEachRow" > data.jsoneachrow

$CLICKHOUSE_LOCAL -q "desc stream stream" < data.jsoneachrow
$CLICKHOUSE_LOCAL -q "select * from stream" < data.jsoneachrow

rm data.jsoneachrow

echo -e "1\t2\t3" | $CLICKHOUSE_LOCAL -q "desc stream stream" --file=-
echo -e "1\t2\t3" | $CLICKHOUSE_LOCAL -q "select * from stream" --file=-

