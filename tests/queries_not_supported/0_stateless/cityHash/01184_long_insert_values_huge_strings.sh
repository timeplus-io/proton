#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop stream if exists huge_strings"
$CLICKHOUSE_CLIENT -q "create stream huge_strings (n uint64, l uint64, s string, h uint64) engine=MergeTree order by n"

# Timeouts are increased, because test can be slow with sanitizers and parallel runs.

for _ in {1..10}; do
  $CLICKHOUSE_CLIENT --receive_timeout 100 --send_timeout 100 --connect_timeout 100 --query "select number, (rand() % 10*1000*1000) as l, repeat(randomString(l/1000/1000), 1000*1000) as s, cityHash64(s) from numbers(10) format Values" | $CLICKHOUSE_CLIENT --receive_timeout 100 --send_timeout 100 --connect_timeout 100 --query "insert into huge_strings values" &
  $CLICKHOUSE_CLIENT --receive_timeout 100 --send_timeout 100 --connect_timeout 100 --query "select number % 10, (rand() % 10) as l, randomString(l) as s, cityHash64(s) from numbers(100000)" | $CLICKHOUSE_CLIENT --receive_timeout 100 --send_timeout 100 --connect_timeout 100 --query "insert into huge_strings format TSV" &
done;
wait

$CLICKHOUSE_CLIENT -q "select count() from huge_strings"
$CLICKHOUSE_CLIENT -q "select sum(l = length(s)) from huge_strings"
$CLICKHOUSE_CLIENT -q "select sum(h = cityHash64(s)) from huge_strings"

$CLICKHOUSE_CLIENT -q "drop stream huge_strings"
