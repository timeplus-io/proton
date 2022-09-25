#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS array)" 2>/dev/null || true;
$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS aggregate_function)" 2>/dev/null || true;
$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS nullable)" 2>/dev/null || true;
$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS tuple)" 2>/dev/null || true;
$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS fixed_string)" 2>/dev/null || true;
$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS Enum)" 2>/dev/null || true;
$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS datetime('UTC'))";

echo

($CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE to_string(dummy)" 2>/dev/null && echo "Expected failure") || true;
($CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE to_string(1)" 2>/dev/null && echo "Expected failure") || true;
($CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE 256" 2>/dev/null && echo "Expected failure") || true;
($CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE -1" 2>/dev/null && echo "Expected failure") || true;
($CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE CAST(256 AS nullable(uint16))" 2>/dev/null && echo "Expected failure") || true;

$CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE CAST(NULL AS nullable(uint8))"
$CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE 255"
$CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE CAST(255 AS nullable(uint8))"
