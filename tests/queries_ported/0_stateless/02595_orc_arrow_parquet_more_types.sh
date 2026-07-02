#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(c1 int128, c2 uint128, c3 int256, c4 uint256, c5 enum8('a' = 1), c6 enum16('b' = 1)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select 42::int128 as c1, 42::uint128 as c2, 42::int256 as c3, 42::uint256 as c4, 'a'::enum8('a' = 1) as c5, 'b'::enum16('b' = 1) as c6 format Parquet" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 FORMAT Parquet"
$CLICKHOUSE_CLIENT -q "select * from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(c1 int128, c2 uint128, c3 int256, c4 uint256, c5 enum8('a' = 1), c6 enum16('b' = 1)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select 42::int128 as c1, 42::uint128 as c2, 42::int256 as c3, 42::uint256 as c4, 'a'::enum8('a' = 1) as c5, 'b'::enum16('b' = 1) as c6 format Arrow" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 FORMAT Arrow"
$CLICKHOUSE_CLIENT -q "select * from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(c1 int128, c2 uint128, c3 int256, c4 uint256, c5 enum8('a' = 1), c6 enum16('b' = 1), c7 Decimal256(2), c8 IPv4) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select 42::int128 as c1, 42::uint128 as c2, 42::int256 as c3, 42::uint256 as c4, 'a'::enum8('a' = 1) as c5, 'b'::enum16('b' = 1) as c6, 42.42::Decimal256(2) as c7, '0.0.0.0'::IPv4 as c8 format ORC" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 FORMAT ORC"
$CLICKHOUSE_CLIENT -q "select * from table1"

# FIXME: NULL::nullable(IPv6) as x format ORC
# $CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
# $CLICKHOUSE_CLIENT -q "CREATE STREAM table1(x nullable(IPv6)) ENGINE = Memory"
# $CLICKHOUSE_CLIENT -q "select NULL::nullable(IPv6) as x format ORC" | $CLICKHOUSE_CLIENT "INSERT INTO table1 FORMAT ORC"
# $CLICKHOUSE_CLIENT -q "select * from table1"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS table1"
$CLICKHOUSE_CLIENT -q "CREATE STREAM table1(x nullable(uint256)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "select NULL::nullable(uint256) as x format ORC" | $CLICKHOUSE_CLIENT -q "INSERT INTO table1 FORMAT ORC"
$CLICKHOUSE_CLIENT -q "select * from table1"
