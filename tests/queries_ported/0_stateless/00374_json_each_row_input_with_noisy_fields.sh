#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS json_noisy"
$CLICKHOUSE_CLIENT -q "create stream json_noisy (d1 uint8, d2 string) "

echo '{"d1" : 1, "d2" : "ok"}
{ }
{"t1" : 0, "t2":true,"t3":false, "t4":null,"t5":[],"t6":"trash" }
{"d2":"ok","t1":[[[]],true, null, false, "1","2",9.03,101], "t2":[["1","2"]], "d1":"1"}
{"d2":"ok","t1":[[[]],true, null, false, "1","2", 0.03, 1], "d1":"1", "t2":["1","2"]}
{"d2":"ok","t1":{"a":{"b": {} ,"c":false},"b":[true,null, false]}, "t2":  { "a": [  ] } , "d1":1}
{"t0" : -0.1, "t1" : +1, "t2" : 0, "t3" : [0.0, -0.1], "d2" : "ok", "d1" : 1}' \
| $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 -q "INSERT INTO json_noisy(d1, d2) FORMAT JSONEachRow"

sleep 3s
$CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT * FROM table(json_noisy) settings asterisk_include_reserved_columns=false"
$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS json_noisy"

# Regular test for datetime

echo
$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS json_each_row"
$CLICKHOUSE_CLIENT -q "create stream json_each_row (d datetime('Europe/Moscow')) "
echo '{"d" : "2017-08-31 18:36:48", "t" : ""}
{"d" : "1504193808", "t" : -1}
{"d" : 1504193808, "t" : []}
{"d" : 01504193808, "t" : []}' \
| $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 -q "INSERT INTO json_each_row FORMAT JSONEachRow"
sleep 3s
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT * FROM table(json_each_row) settings asterisk_include_reserved_columns=false"
$CLICKHOUSE_CLIENT -q "DROP STREAM json_each_row"
