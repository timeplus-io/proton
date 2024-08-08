#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS csv";
$CLICKHOUSE_CLIENT --query="create stream csv (s string, n uint64 DEFAULT 1, d date DEFAULT '2019-06-19') ";

printf '"Hello, world", 123, "2016-01-01"
"Hello, ""world""", "456", 2016-01-02,
Hello "world", 789 ,2016-01-03
"Hello
 world", 100, 2016-01-04,
 default,,
 default-eof,,' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=1 --input_format_csv_empty_as_default=1 --query="INSERT INTO csv(s,n,d) FORMAT CSV";

sleep 2
$CLICKHOUSE_CLIENT --query="SELECT * FROM table(csv) ORDER BY d settings asterisk_include_reserved_columns=false";
$CLICKHOUSE_CLIENT --query="DROP STREAM csv";

$CLICKHOUSE_CLIENT --query="create stream csv (t datetime('Europe/Moscow'), s string) ";
sleep 2s

echo '"2016-01-01 01:02:03","1"
2016-01-02 01:02:03, "2"
1502792101,"3"
99999,"4"' | $CLICKHOUSE_CLIENT --query="INSERT INTO csv(t,s) FORMAT CSV";

sleep 2s

$CLICKHOUSE_CLIENT --query="SELECT * FROM table(csv) ORDER BY s settings asterisk_include_reserved_columns=false";
$CLICKHOUSE_CLIENT --query="DROP STREAM csv";
sleep 2s


$CLICKHOUSE_CLIENT --query="create stream csv (t nullable(datetime('Europe/Moscow')), s nullable(string)) ";
sleep 2s

echo 'NULL, NULL
"2016-01-01 01:02:03",NUL
"2016-01-02 01:02:03",Nhello' | $CLICKHOUSE_CLIENT --format_csv_null_representation='NULL' --input_format_csv_empty_as_default=1 --query="INSERT INTO csv(t,s) FORMAT CSV";
sleep 4s

$CLICKHOUSE_CLIENT --query="SELECT * FROM table(csv) ORDER BY s NULLS LAST settings asterisk_include_reserved_columns=false";
$CLICKHOUSE_CLIENT --query="DROP STREAM csv";
