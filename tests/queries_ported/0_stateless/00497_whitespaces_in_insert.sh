#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS ws";
$CLICKHOUSE_CLIENT -q "create stream ws (i uint8) ";

$CLICKHOUSE_CLIENT -q "INSERT INTO ws(i) FORMAT RowBinary ;";
$CLICKHOUSE_CLIENT -q "INSERT INTO ws(i) FORMAT RowBinary 	; ";
$CLICKHOUSE_CLIENT -q "INSERT INTO ws(i) FORMAT RowBinary
; ";
echo -n ";" | $CLICKHOUSE_CLIENT -q "INSERT INTO ws(i) FORMAT RowBinary";
sleep 2
$CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT * FROM table(ws) settings asterisk_include_reserved_columns=false";
$CLICKHOUSE_CLIENT -q "DROP STREAM ws";


$CLICKHOUSE_CLIENT -q "SELECT ''";


$CLICKHOUSE_CLIENT -q "create stream ws (s string) ";
$CLICKHOUSE_CLIENT -q "INSERT INTO ws(s) FORMAT TSV	;
";
echo ";" | $CLICKHOUSE_CLIENT -q "INSERT INTO ws(s) FORMAT TSV"
if $CLICKHOUSE_CLIENT -q "INSERT INTO ws(s) FORMAT TSV;" 1>/dev/null 2>/dev/null; then
    echo ERROR;
fi
sleep 2
$CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT * FROM table(ws) settings asterisk_include_reserved_columns=false";

$CLICKHOUSE_CLIENT -q "DROP STREAM ws";
