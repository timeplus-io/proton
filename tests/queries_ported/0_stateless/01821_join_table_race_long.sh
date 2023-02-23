#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS join_table_race"
$CLICKHOUSE_CLIENT -q "CREATE STREAM join_table_race(id int32, name string) ENGINE = Join(ANY, LEFT, id)"

for _ in {0..100}; do $CLICKHOUSE_CLIENT -q "INSERT INTO join_table_race VALUES ($RANDOM, '$RANDOM')" > /dev/null 2> /dev/null; done &

for _ in {0..200}; do $CLICKHOUSE_CLIENT -q "SELECT count() FROM join_table_race FORMAT Null" > /dev/null 2> /dev/null; done &

for _ in {0..100}; do $CLICKHOUSE_CLIENT -q "TRUNCATE STREAM join_table_race" > /dev/null 2> /dev/null; done &

for _ in {0..100}; do $CLICKHOUSE_CLIENT -q "ALTER STREAM join_table_race DELETE WHERE id % 2 = 0" > /dev/null 2> /dev/null; done &

wait

$CLICKHOUSE_CLIENT -q "TRUNCATE STREAM join_table_race"
$CLICKHOUSE_CLIENT -q "INSERT INTO join_table_race VALUES (1, 'foo')"
$CLICKHOUSE_CLIENT -q "SELECT id, name FROM join_table_race"

$CLICKHOUSE_CLIENT -q "DROP STREAM join_table_race"
