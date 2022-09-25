#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS orc_lc";


$CLICKHOUSE_CLIENT --query="create stream orc_lc (lc low_cardinality(string), array_lc array(low_cardinality(string)), tuple_lc  tuple(low_cardinality(string))) ()";


$CLICKHOUSE_CLIENT --query="SELECT [lc] as array_lc, tuple(lc) as tuple_lc, toLowCardinality(to_string(number)) as lc from numbers(10) FORMAT ORC" | $CLICKHOUSE_CLIENT --query="INSERT INTO orc_lc FORMAT ORC";

$CLICKHOUSE_CLIENT --query="SELECT * FROM orc_lc";
