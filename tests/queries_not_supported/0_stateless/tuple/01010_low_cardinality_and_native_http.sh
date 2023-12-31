#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query="drop stream if exists tab_str";
$CLICKHOUSE_CLIENT --query="drop stream if exists tab_str_lc";

$CLICKHOUSE_CLIENT --query="create stream tab_str (x string) engine = MergeTree order by tuple()";
$CLICKHOUSE_CLIENT --query="create stream tab_str_lc (x low_cardinality(string)) engine = MergeTree order by tuple()";
$CLICKHOUSE_CLIENT --query="insert into tab_str values ('abc')";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=select+x+from+tab_str+format+Native" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+tab_str_lc+FORMAT+Native" --data-binary @-

$CLICKHOUSE_CLIENT --query="select x from tab_str_lc";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=select+x+from+tab_str_lc+format+Native" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+tab_str+FORMAT+Native" --data-binary @-

$CLICKHOUSE_CLIENT --query="select '----'";
$CLICKHOUSE_CLIENT --query="select x from tab_str";

$CLICKHOUSE_CLIENT -q "DROP STREAM tab_str"
$CLICKHOUSE_CLIENT -q "DROP STREAM tab_str_lc"
