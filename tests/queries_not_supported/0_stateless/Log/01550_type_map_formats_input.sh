#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS map_formats_input"
$CLICKHOUSE_CLIENT -q "CREATE STREAM map_formats_input (m map(string, uint32), m1 map(string, Date), m2 map(string, array(uint32))) ENGINE = Log;" --allow_experimental_map_type 1

$CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT JSONEachRow" <<< '{"m":{"k1":1,"k2":2,"k3":3},"m1":{"k1":"2020-05-05"},"m2":{"k1":[],"k2":[7,8]}}'
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"
$CLICKHOUSE_CLIENT -q "TRUNCATE STREAM map_formats_input"

$CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT CSV" <<< "\"{'k1':1,'k2':2,'k3':3}\",\"{'k1':'2020-05-05'}\",\"{'k1':[],'k2':[7,8]}\""
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"
$CLICKHOUSE_CLIENT -q "TRUNCATE STREAM map_formats_input"

$CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT TSV" <<< "{'k1':1,'k2':2,'k3':3}	{'k1':'2020-05-05'}	{'k1':[],'k2':[7,8]}"
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"

$CLICKHOUSE_CLIENT -q 'SELECT * FROM map_formats_input FORMAT Native' | $CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT Native"
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"

$CLICKHOUSE_CLIENT -q "DROP STREAM map_formats_input"
