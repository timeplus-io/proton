#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(x low_cardinality(string)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select to_low_cardinality(to_string(number % 10)) as x from numbers(20) format Arrow settings max_block_size=7, output_format_arrow_low_cardinality_as_dictionary=1" | $CLICKHOUSE_CLIENT -q "insert into table1 format Arrow"
$CLICKHOUSE_CLIENT -q "select * from table1 order by x"
