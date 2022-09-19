#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS csv";

$CLICKHOUSE_CLIENT --query="create stream csv (s string, n uint64, d date) ";

echo "'single quote' not end, 123, 2016-01-01
'em good, 456, 2016-01-02" | $CLICKHOUSE_CLIENT --format_csv_allow_single_quotes=0 --query="INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY d";

$CLICKHOUSE_CLIENT --query="DROP STREAM csv";

$CLICKHOUSE_CLIENT --query="create stream csv (s string, n uint64, d date) ";

echo "'single quote' not end, 123, 2016-01-01
'em good, 456, 2016-01-02" | $CLICKHOUSE_CLIENT --multiquery --query="SET format_csv_allow_single_quotes=0; INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY d";

$CLICKHOUSE_CLIENT --query="DROP STREAM csv";

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS csv";

$CLICKHOUSE_CLIENT --query="create stream csv (s string, n uint64, d date) ";

echo '"double quote" not end, 123, 2016-01-01
"em good, 456, 2016-01-02' | $CLICKHOUSE_CLIENT --format_csv_allow_double_quotes=0 --query="INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY d";

$CLICKHOUSE_CLIENT --query="DROP STREAM csv";

$CLICKHOUSE_CLIENT --query="create stream csv (s string, n uint64, d date) ";

echo '"double quote" not end, 123, 2016-01-01
"em good, 456, 2016-01-02' | $CLICKHOUSE_CLIENT --multiquery --query="SET format_csv_allow_double_quotes=0; INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY d";

$CLICKHOUSE_CLIENT --query="DROP STREAM csv";
