#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create stream IF NOT EXISTS default_table (x uint32, y uint32 DEFAULT 42, z uint32 DEFAULT 33) ;"

echo "(DEFAULT, 1, DEFAULT), (1, DEFAULT, 2)" | $CLICKHOUSE_CLIENT --input_format_values_interpret_expressions=0 -q "INSERT INTO default_table FORMAT Values"
echo "(2, DEFAULT), (3, 3)" | $CLICKHOUSE_CLIENT --input_format_values_interpret_expressions=0 -q "INSERT INTO default_table(x, z) FORMAT Values"

echo "(DEFAULT, DEFAULT, DEFAULT), (4, DEFAULT, 3)" | $CLICKHOUSE_CLIENT --input_format_values_interpret_expressions=1 -q "INSERT INTO default_table FORMAT Values"
echo "(5, DEFAULT), (6, 6)" | $CLICKHOUSE_CLIENT --input_format_values_interpret_expressions=1 -q "INSERT INTO default_table(x, y) FORMAT Values"

$CLICKHOUSE_CLIENT --query="SELECT * FROM default_table ORDER BY x, y";
$CLICKHOUSE_CLIENT --query="DROP STREAM default_table"
