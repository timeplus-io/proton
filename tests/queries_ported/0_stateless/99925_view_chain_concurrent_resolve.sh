#!/usr/bin/env bash
# Tags: race, long
# Reproducer for the StorageView analyzer race: concurrent queries against a 2-deep
# view chain (inner view: tumble + group_array(tuple) + ARRAY JOIN; outer view: lag OVER)
# combined with CREATE OR REPLACE churn on the outer view. Pre-fix the server SEGV'd
# within seconds; post-fix it must stay up.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o pipefail

DB="${CLICKHOUSE_DATABASE}_99925"
CLIENT="$CLICKHOUSE_CLIENT"

$CLIENT --query "DROP DATABASE IF EXISTS $DB"
$CLIENT --query "CREATE DATABASE $DB"
trap '$CLIENT --query "DROP DATABASE IF EXISTS $DB CASCADE" >/dev/null 2>&1 || true' EXIT
$CLIENT --query "CREATE STREAM $DB.data (time datetime64(3,'UTC'), key string, val float64)"
$CLIENT --query "INSERT INTO $DB.data (time, key, val) SELECT
    now64(3,'UTC') - interval (number % 5) second AS time,
    'K_' || to_string(number % 3) AS key,
    number * 0.1 AS val FROM numbers(60)"

$CLIENT --query "CREATE VIEW $DB.v1 AS
    WITH b AS (SELECT window_start AS time,
        array_sort(t -> t.2, group_array((key, val))) AS s,
        length(group_array(key)) AS n
        FROM tumble($DB.data, 1s) GROUP BY window_start)
    SELECT time, s[i].1 AS key,
        cast(i - 1, 'float64') / null_if(n - 1, 0) - 0.5 AS rv,
        s[i].2 AS val
    FROM b ARRAY JOIN array_enumerate(s) AS i"

$CLIENT --query "CREATE VIEW $DB.v2 AS
    SELECT time, key, rv, val,
        lag(rv) OVER (PARTITION BY key) AS prev,
        lag(rv) OVER (PARTITION BY key) * val AS pnl
    FROM $DB.v1"

QUERY="EXPLAIN SELECT count() FROM $DB.v2 WHERE pnl IS NOT NULL"
CHURN="CREATE OR REPLACE VIEW $DB.v2 AS SELECT time, key, rv, val,
    lag(rv) OVER (PARTITION BY key) AS prev,
    lag(rv) OVER (PARTITION BY key) * val AS pnl FROM $DB.v1"

# Smoke-check both statements before swallowing stderr in the race loops,
# so a typo can't make the whole test pass vacuously.
$CLIENT --query "$CHURN"
$CLIENT --query "$QUERY" >/dev/null

# Batched multi-statement scripts via -n keep client startup cost flat while
# still exercising the analyzer (StorageView::isStreamingQuery /
# dataStreamSemantic / hasStreamingGlobalAggregation) — that's the path that
# race-fault'd pre-fix. Concurrency comes from N parallel client processes,
# CHURN interleaves CREATE OR REPLACE on the outer view.
CHURN_SCRIPT=$(for _ in {1..20}; do printf '%s;\n' "$CHURN"; done)
QUERY_SCRIPT=$(for _ in {1..12}; do printf '%s;\n' "$QUERY"; done)

(
    echo "$CHURN_SCRIPT" | $CLIENT -n >/dev/null 2>&1
) &
CHURN_PID=$!

QUERY_PIDS=()
for _ in {1..5}; do
    (echo "$QUERY_SCRIPT" | $CLIENT -n >/dev/null 2>&1) &
    QUERY_PIDS+=($!)
done

wait "$CHURN_PID" "${QUERY_PIDS[@]}"

# Server must still respond.
$CLIENT --query "SELECT 'alive'"
