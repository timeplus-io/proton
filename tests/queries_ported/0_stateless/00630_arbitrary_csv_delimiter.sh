#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS csv";
$CLICKHOUSE_CLIENT --query="create stream csv (s string, n uint64, d date) ";

echo '"Hello, world"| 123| "2016-01-01"
"Hello, ""world"""| "456"| 2016-01-02|
Hello "world"| 789 |2016-01-03
"Hello
 world"| 100| 2016-01-04|' | $CLICKHOUSE_CLIENT --format_csv_delimiter="|"  --query="INSERT INTO csv(s,n,d) FORMAT CSV";

sleep 3;

$CLICKHOUSE_CLIENT --query="SELECT * except _tp_time FROM csv ORDER BY d settings query_mode='table'";

$CLICKHOUSE_CLIENT --query="DROP STREAM csv";
$CLICKHOUSE_CLIENT --query="create stream csv (s string, n uint64, d date) ";

echo '"Hello, world"; 123; "2016-01-01"
"Hello, ""world"""; "456"; 2016-01-02;
Hello "world"; 789 ;2016-01-03
"Hello
 world"; 100; 2016-01-04;' | $CLICKHOUSE_CLIENT --multiquery --query="SET format_csv_delimiter=';'; INSERT INTO csv(s,n,d) FORMAT CSV";

sleep 3;
$CLICKHOUSE_CLIENT --query="SELECT * except _tp_time  FROM csv ORDER BY d settings query_mode='table'";
$CLICKHOUSE_CLIENT --format_csv_delimiter=";" --query="SELECT * except _tp_time  FROM csv ORDER BY d FORMAT CSV settings query_mode='table'";
$CLICKHOUSE_CLIENT --format_csv_delimiter="/" --query="SELECT * except _tp_time  FROM csv ORDER BY d FORMAT CSV settings query_mode='table'";

$CLICKHOUSE_CLIENT --query="DROP STREAM csv";
$CLICKHOUSE_CLIENT --query="create stream csv (s1 string, s2 string) ";

echo 'abc,def;hello;
hello; world;
"hello ""world""";abc,def;' | $CLICKHOUSE_CLIENT --multiquery --query="SET format_csv_delimiter=';'; INSERT INTO csv(s1,s2) FORMAT CSV";

sleep 3;

$CLICKHOUSE_CLIENT --query="SELECT * except _tp_time  FROM csv settings query_mode='table'";

$CLICKHOUSE_CLIENT --query="DROP STREAM csv";
$CLICKHOUSE_CLIENT --query="create stream csv (s1 string, s2 string) ";

echo '"s1";"s2"
abc,def;hello;
hello; world;
"hello ""world""";abc,def;' | $CLICKHOUSE_CLIENT --multiquery --query="SET format_csv_delimiter=';'; INSERT INTO csv(s1,s2) FORMAT CSVWithNames";

sleep 3;

$CLICKHOUSE_CLIENT --format_csv_delimiter=";" --query="SELECT * except _tp_time  FROM csv FORMAT CSV settings query_mode='table'";
$CLICKHOUSE_CLIENT --format_csv_delimiter="," --query="SELECT * except _tp_time  FROM csv FORMAT CSV settings query_mode='table'";
$CLICKHOUSE_CLIENT --format_csv_delimiter="/" --query="SELECT * except _tp_time  FROM csv FORMAT CSV settings query_mode='table'";

$CLICKHOUSE_CLIENT --query="DROP STREAM csv";
