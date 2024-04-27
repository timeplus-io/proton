#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create stream tab (x uint64, y string) engine = MergeTree order by x"
for _ in $(seq 1 100); do timeout -s 2 0.05 $CLICKHOUSE_CLIENT --interactive_delay 1000 -q "insert into tab select number, to_string(number) from system.numbers" || true; done

$CLICKHOUSE_CLIENT -q "create stream tap (x uint64, y string)"
for _ in $(seq 1 100); do timeout -s 2 0.05 $CLICKHOUSE_CLIENT --interactive_delay 1000 -q "insert into tap(x, y) select number, to_string(number) from system.numbers" || true; done
