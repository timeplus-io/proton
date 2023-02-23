#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS test_date_out_of_range sync";
$CLICKHOUSE_CLIENT --query="CREATE STREAM test_date_out_of_range (f string, t Date) engine=Memory()";

printf '"above", 2200-12-31
"below", 1900-01-01
' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=1 --input_format_csv_empty_as_default=1 --query="INSERT INTO test_date_out_of_range FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM test_date_out_of_range";
$CLICKHOUSE_CLIENT --query="DROP STREAM test_date_out_of_range";