#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS t_index_hypothesis"

$CLICKHOUSE_CLIENT -q "create stream t_index_hypothesis (a uint32, b uint32, INDEX t a != b TYPE hypothesis GRANULARITY 1) ENGINE = MergeTree ORDER BY a"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_index_hypothesis SELECT number, number + 1 FROM numbers(10000000)"

for _ in {0..30}; do
    output=`$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_index_hypothesis WHERE a = b"`
    if [[ $output != "0" ]]; then
        echo "output: $output, expected: 0"
        exit 1
    fi
done

echo OK

$CLICKHOUSE_CLIENT -q "DROP STREAM t_index_hypothesis"
