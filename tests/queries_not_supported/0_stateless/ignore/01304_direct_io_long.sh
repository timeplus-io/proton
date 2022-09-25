#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP STREAM IF EXISTS bug;
    create stream bug (UserID uint64, date date) ENGINE = MergeTree ORDER BY date;
    INSERT INTO bug SELECT rand64(), '2020-06-07' FROM numbers(50000000);
    OPTIMIZE STREAM bug FINAL;"
LOG="$CLICKHOUSE_TMP/err-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_BENCHMARK --iterations 10 --max_threads 100 --min_bytes_to_use_direct_io 1 <<< "SELECT sum(UserID) FROM bug PREWHERE NOT ignore(date)" 1>/dev/null 2>"$LOG"
cat "$LOG" | grep Exception
cat "$LOG" | grep Loaded

rm "$LOG"

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP STREAM bug;"
