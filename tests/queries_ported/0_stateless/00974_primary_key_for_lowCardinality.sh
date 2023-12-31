#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS lowString;"
$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS string;"

$CLICKHOUSE_CLIENT -n --query="
create stream lowString
(
a low_cardinality(string),
b Date
)
ENGINE = MergeTree()
PARTITION BY to_YYYYMM(b)
ORDER BY (a)"

$CLICKHOUSE_CLIENT -n --query="
create stream string
(
a string,
b Date
)
ENGINE = MergeTree()
PARTITION BY to_YYYYMM(b)
ORDER BY (a)"

$CLICKHOUSE_CLIENT --query="insert into lowString (a, b) select top 100000 to_string(number), today() from system.numbers"

$CLICKHOUSE_CLIENT --query="insert into string (a, b) select top 100000 to_string(number), today() from system.numbers"

$CLICKHOUSE_CLIENT --query="select count() from lowString where a in ('1', '2') FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="select count() from string where a in ('1', '2') FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="DROP STREAM lowString;"
$CLICKHOUSE_CLIENT --query="DROP STREAM string;"
