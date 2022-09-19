#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/1300

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS advertiser";
$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS advertiser_test";
$CLICKHOUSE_CLIENT -q "create stream advertiser ( action_date date, adblock uint8, imps int64 ) Engine = SummingMergeTree( action_date, ( adblock ), 8192, ( imps ) )";
$CLICKHOUSE_CLIENT -q "create stream advertiser_test ( action_date date, adblock uint8, imps int64, Hash uint64 ) Engine = SummingMergeTree( action_date, ( adblock, Hash ), 8192, ( imps ) )";

# This test will fail. It's ok.
$CLICKHOUSE_CLIENT -q "INSERT INTO advertiser_test SELECT *, sipHash64( CAST(adblock  AS string) ), CAST(1 AS int8) FROM advertiser;" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP STREAM advertiser";
$CLICKHOUSE_CLIENT -q "DROP STREAM advertiser_test";
$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'";
