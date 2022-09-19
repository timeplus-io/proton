#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="SELECT [1,]::array(uint8)"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [1, 2]]::array(uint8)"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [[1, 2]::array(uint8)"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [[1, 2],, []]::array(array(uint8))"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [[1, 2][]]::array(array(uint8))"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [1,,2]::array(uint8)"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [1 2]::array(uint8)"  2>&1 | grep -o -m1 'Syntax error'

$CLICKHOUSE_CLIENT --query="SELECT 1 4::uint32"  2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT '1' '4'::uint32"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT '1''4'::uint32"  2>&1 | grep -o -m1 'Code: 6'

$CLICKHOUSE_CLIENT --query="SELECT ::uint32"  2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT ::string"  2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT -::int32"  2>&1 | grep -o 'Syntax error'

$CLICKHOUSE_CLIENT --query="SELECT [1, -]::array(int32)"  2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [1, 3-]::array(int32)"  2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [-, 2]::array(int32)"  2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [--, 2]::array(int32)"  2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [1, 2]-::array(int32)"  2>&1 | grep -o 'Syntax error'
