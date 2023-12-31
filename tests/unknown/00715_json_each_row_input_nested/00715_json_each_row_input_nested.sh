#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS json_each_row_nested"
$CLICKHOUSE_CLIENT -q "create stream json_each_row_nested (d1 uint8, d2 string, n nested (s string, i int32) ) "

echo '{"d1" : 1, "d2" : "ok", "n.s" : ["abc", "def"], "n.i" : [1, 23]}
{ }
{"t1" : 0, "n.t2":true,"n.i":[45, 67, 8], "n.s":["x", "y", "z"],"t5":[],"t6":"trash" }
{"d2":"ok","n.s":["dog", "cat", "pig"], "n.x":[["1","2"]], "d1":"1", "n.i":[3, 3, 3]}
{"t0" : -0.1, "n.s" : ["zero","negative one"], "a.b" : 0, "n.i" : [0, -1], "d2" : "ok", "d1" : 1}' \
| $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 -q "INSERT INTO json_each_row_nested(d1,d2,n.s,n.i) FORMAT JSONEachRow"

sleep 3;

$CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT * FROM json_each_row_nested settings query_mode='false'"

test_nested_json()
{
$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS json_each_row_nested"

$CLICKHOUSE_CLIENT -q "create stream json_each_row_nested (d1 uint8, d2 string, n nested (s string, i int32) ) "

echo '{"d1" : 1, "d2" : "ok", "n" : { "s" : ["abc", "def"], "i" : [1, 23]} }
{ }
{"t1" : 0, "n.t2":true,"n" : {"i":[45, 67, 8], "s":["x", "y", "z"]}, "t5":[],"t6":"trash" }
{"d2":"ok","n" : {"s":["dog", "cat", "pig"], "x":[["1","2"]], "i":[3, 3, 3]}, "d1":"1", "n.j":[4, 4, 4]}
{"t0" : -0.1, "n": {"s" : ["zero","negative one"], "i" : [0, -1]}, "d2" : "ok", "d1" : 1}' \
| $CLICKHOUSE_CLIENT "$@" --input_format_skip_unknown_fields=1 -q "INSERT INTO json_each_row_nested(d1,d2,n.s,n.i) FORMAT JSONEachRow"

sleep 3;

$CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT * FROM json_each_row_nested settings query_mode='false'"
$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS json_each_row_nested"
}

test_nested_json
test_nested_json --input_format_import_nested_json=1
