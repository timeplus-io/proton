#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS empty_strings_deserialization"
$CLICKHOUSE_CLIENT -q "create stream empty_strings_deserialization(s string, i int32, f float32) ENGINE Memory"

echo ',,' | $CLICKHOUSE_CLIENT -q "INSERT INTO empty_strings_deserialization(s,i,f) FORMAT CSV"
echo 'aaa,,' | $CLICKHOUSE_CLIENT -q "INSERT INTO empty_strings_deserialization(s,i,f) FORMAT CSV"
echo 'bbb,,-0' | $CLICKHOUSE_CLIENT -q "INSERT INTO empty_strings_deserialization(s,i,f) FORMAT CSV"

sleep 2
$CLICKHOUSE_CLIENT -q "SELECT * FROM empty_strings_deserialization ORDER BY s settings asterisk_include_reserved_columns=false"

$CLICKHOUSE_CLIENT -q "DROP STREAM empty_strings_deserialization"
