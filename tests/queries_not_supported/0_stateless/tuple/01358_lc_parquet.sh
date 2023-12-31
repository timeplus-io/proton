#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop stream if exists test_lc"
$CLICKHOUSE_CLIENT -q "drop stream if exists test"
$CLICKHOUSE_CLIENT -q "create stream test (a string) Engine = MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "create stream test_lc (a low_cardinality(string)) Engine = MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "select 'abc' as a format Parquet" | $CLICKHOUSE_CLIENT -q "insert into test_lc format Parquet"
$CLICKHOUSE_CLIENT -q "select a from test_lc format Parquet" | $CLICKHOUSE_CLIENT -q "insert into test format Parquet"
$CLICKHOUSE_CLIENT -q "select a from test order by a"
$CLICKHOUSE_CLIENT -q "drop stream if exists test_lc"
$CLICKHOUSE_CLIENT -q "drop stream if exists test"

$CLICKHOUSE_CLIENT -q "drop stream if exists test_lc"
$CLICKHOUSE_CLIENT -q "drop stream if exists test"
$CLICKHOUSE_CLIENT -q "create stream test (a Nullable(string)) Engine = MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "create stream test_lc (a low_cardinality(Nullable(string))) Engine = MergeTree order by tuple()"
$CLICKHOUSE_CLIENT -q "select 'ghi' as a format Parquet" | $CLICKHOUSE_CLIENT -q "insert into test_lc format Parquet"
$CLICKHOUSE_CLIENT -q "select cast(Null as Nullable(string)) as a format Parquet" | $CLICKHOUSE_CLIENT -q "insert into test_lc format Parquet"
$CLICKHOUSE_CLIENT -q "select a from test_lc format Parquet" | $CLICKHOUSE_CLIENT -q "insert into test format Parquet"
$CLICKHOUSE_CLIENT -q "select a from test order by a"
$CLICKHOUSE_CLIENT -q "drop stream if exists test_lc"
$CLICKHOUSE_CLIENT -q "drop stream if exists test"
