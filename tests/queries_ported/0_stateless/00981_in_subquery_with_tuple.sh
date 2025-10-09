#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS bug";
$CLICKHOUSE_CLIENT --query="SELECT sleep(1)";
$CLICKHOUSE_CLIENT --query="CREATE STREAM bug (d date, s string)";
$CLICKHOUSE_CLIENT --query="INSERT INTO bug(d, s) VALUES ('2019-08-09', 'hello'), ('2019-08-10', 'world'), ('2019-08-11', 'world'), ('2019-08-12', 'hello')";
$CLICKHOUSE_CLIENT --query="SELECT sleep(2)";

#SET force_primary_key = 1;

$CLICKHOUSE_CLIENT --query="SELECT * FROM table(bug) WHERE (s, d) IN (SELECT (s, max(d)) FROM table(bug) GROUP BY s) ORDER BY d" 2>&1 | grep "Number of columns in section IN doesn't match" > /dev/null && echo "OK1";
$CLICKHOUSE_CLIENT --query="SELECT * FROM table(bug) WHERE (s, d, s) IN (SELECT s, max(d) FROM table(bug) GROUP BY s)" 2>&1 | grep "Number of columns in section IN doesn't match" > /dev/null && echo "OK2";
$CLICKHOUSE_CLIENT --query="SELECT * FROM table(bug) WHERE (s, d) IN (SELECT s, max(d), s FROM table(bug) GROUP BY s)" 2>&1 | grep "Number of columns in section IN doesn't match" > /dev/null && echo "OK3";

$CLICKHOUSE_CLIENT --query="SELECT * FROM table(bug) WHERE (s, toDateTime(d)) IN (SELECT s, max(d) FROM table(bug) GROUP BY s)" 2>&1 | grep "Types of column 2 in section IN don't match" > /dev/null && echo "OK4";
$CLICKHOUSE_CLIENT --query="SELECT * FROM table(bug) WHERE (s, d) IN (SELECT s, toDateTime(max(d)) FROM table(bug) GROUP BY s)" 2>&1 | grep "Types of column 2 in section IN don't match" > /dev/null && echo "OK5";

$CLICKHOUSE_CLIENT --query="SELECT d, s FROM table(bug) WHERE (s, d) IN (SELECT s, max(d) FROM table(bug) GROUP BY s) ORDER BY d";

$CLICKHOUSE_CLIENT --query="DROP STREAM bug";