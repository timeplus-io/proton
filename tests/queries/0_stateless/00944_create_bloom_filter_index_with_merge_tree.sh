#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

for sequence in 1 10 100 1000 10000 100000 1000000 10000000 100000000 1000000000; do \
rate=$(echo "1 $sequence" | awk '{printf("%0.9f\n",$1/$2)}')
$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS bloom_filter_idx";
$CLICKHOUSE_CLIENT --query="create stream bloom_filter_idx ( u64 uint64, i32 int32, f64 float64, d Decimal(10, 2), s string, e Enum8('a' = 1, 'b' = 2, 'c' = 3), dt date, INDEX bloom_filter_a i32 TYPE bloom_filter($rate) GRANULARITY 1 ) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192"
done

$CLICKHOUSE_CLIENT --query="DROP STREAM bloom_filter_idx";
