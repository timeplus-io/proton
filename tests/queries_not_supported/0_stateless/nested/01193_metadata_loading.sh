#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# it is the worst way of making performance test, nevertheless it can detect significant slowdown and some other issues, that usually found by stress test

db="test_01193_$RANDOM"
tables=1000
threads=10
count_multiplier=1
max_time_ms=1000

debug_or_sanitizer_build=$($CLICKHOUSE_CLIENT -q "WITH ((SELECT value FROM system.build_options WHERE name='BUILD_TYPE') AS build, (SELECT value FROM system.build_options WHERE name='CXX_FLAGS') as flags) SELECT build='Debug' OR flags LIKE '%fsanitize%' OR hasThreadFuzzer()")

if [[ debug_or_sanitizer_build -eq 1 ]]; then tables=100; count_multiplier=10; max_time_ms=1500; fi

create_tables() {
  $CLICKHOUSE_CLIENT -q "WITH
          'create stream $db.table_$1_' AS create1,
          ' (i uint64, d date, s string, n nested(i uint8, f Float32)) ENGINE=' AS create2,
          ['Memory', 'File(CSV)', 'Log', 'StripeLog', 'MergeTree ORDER BY i'] AS engines,
          'INSERT INTO $db.table_$1_' AS insert1,
          ' VALUES (0, ''2020-06-25'', ''hello'', [1, 2], [3, 4]), (1, ''2020-06-26'', ''word'', [10, 20], [30, 40])' AS insert2
      SELECT array_string_concat(
          group_array(
              create1 || to_string(number) || create2 || engines[1 + number % length(engines)] || ';\n' ||
              insert1 ||  to_string(number) || insert2
          ), ';\n') FROM numbers($tables) FORMAT TSVRaw;" | $CLICKHOUSE_CLIENT -nm
}

$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db"

for i in $(seq 1 $threads); do
  create_tables "$i" &
done
wait

$CLICKHOUSE_CLIENT -q "create stream $db.table_merge (i uint64, d date, s string, n nested(i uint8, f Float32)) ENGINE=Merge('$db', '^table_')"
$CLICKHOUSE_CLIENT -q "SELECT count() * $count_multiplier, i, d, s, n.i, n.f FROM merge('$db', '^table_9') GROUP BY i, d, s, n.i, n.f ORDER BY i"

for i in {1..5}; do
  $CLICKHOUSE_CLIENT -q "DETACH DATABASE $db"
  $CLICKHOUSE_CLIENT -q "ATTACH DATABASE $db" --query_id="$db-$i";
done

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q "SELECT if(quantile(0.5)(query_duration_ms) < $max_time_ms, 'ok', to_string(group_array(query_duration_ms))) FROM system.query_log WHERE current_database = currentDatabase() AND query_id LIKE '$db-%' AND type=2"

$CLICKHOUSE_CLIENT -q "SELECT count() * $count_multiplier, i, d, s, n.i, n.f FROM $db.table_merge GROUP BY i, d, s, n.i, n.f ORDER BY i"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db"
