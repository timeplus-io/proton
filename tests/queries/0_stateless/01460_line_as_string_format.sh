#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS line_as_string1";
$CLICKHOUSE_CLIENT --query="create stream line_as_string1(field string) ";

echo '"id" : 1,
"date" : "01.01.2020",
"string" : "123{{{\"\\",
"array" : [1, 2, 3],

Finally implement this new feature.' | $CLICKHOUSE_CLIENT --query="INSERT INTO line_as_string1 FORMAT LineAsString";

$CLICKHOUSE_CLIENT --query="SELECT * FROM line_as_string1";
$CLICKHOUSE_CLIENT --query="DROP STREAM line_as_string1"

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS line_as_string2";
$CLICKHOUSE_CLIENT --query="create stream line_as_string2(
    a uint64 default 42,
    b string materialized to_string(a),
    c string
) engine=MergeTree() order by tuple();";

$CLICKHOUSE_CLIENT --query="INSERT INTO line_as_string2(c) values ('ClickHouse')";

echo 'ClickHouse is a `fast` #open-source# (OLAP) database "management" :system:' | $CLICKHOUSE_CLIENT --query="INSERT INTO line_as_string2(c) FORMAT LineAsString";

$CLICKHOUSE_CLIENT --query="SELECT * FROM line_as_string2 order by c";
$CLICKHOUSE_CLIENT --query="DROP STREAM line_as_string2"

$CLICKHOUSE_CLIENT --query="select repeat('aaa',50) from numbers(1000000)" > "${CLICKHOUSE_TMP}"/data1
$CLICKHOUSE_CLIENT --query="create stream line_as_string3(field string) ";
$CLICKHOUSE_CLIENT --query="INSERT INTO line_as_string3 FORMAT LineAsString" < "${CLICKHOUSE_TMP}"/data1
$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM line_as_string3";
$CLICKHOUSE_CLIENT --query="DROP STREAM line_as_string3"

$CLICKHOUSE_CLIENT --query="select randomString(50000) FROM numbers(1000)" > "${CLICKHOUSE_TMP}"/data2
$CLICKHOUSE_CLIENT --query="create stream line_as_string4(field string) ";
$CLICKHOUSE_CLIENT --query="INSERT INTO line_as_string4 FORMAT LineAsString" < "${CLICKHOUSE_TMP}"/data2
$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM line_as_string4";
$CLICKHOUSE_CLIENT --query="DROP STREAM line_as_string4"
