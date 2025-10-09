#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS minmax_idx;"


$CLICKHOUSE_CLIENT -n --query="
CREATE STREAM minmax_idx
(
    u64 uint64,
    i64 int64,
    i32 int32
)
PARTITION BY i32
ORDER BY u64
SETTINGS index_granularity = 2;"

sleep 2

$CLICKHOUSE_CLIENT --query="INSERT INTO minmax_idx(u64, i64, i32) VALUES
(0, 2, 1),
(1, 1, 1),
(2, 1, 1),
(3, 1, 1),
(4, 2, 2),
(5, 2, 2),
(6, 2, 2),
(7, 2, 2),
(8, 1, 2),
(9, 1, 2)"

sleep 2

$CLICKHOUSE_CLIENT --query="SELECT count() FROM table(minmax_idx) WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM table(minmax_idx) WHERE i64 = 2 FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -n --query="
ALTER STREAM minmax_idx ADD INDEX idx (i64, u64 * i64) TYPE minmax GRANULARITY 1 SETTINGS mutations_sync = 2;"

$CLICKHOUSE_CLIENT --query="ALTER STREAM minmax_idx MATERIALIZE INDEX idx IN PARTITION 1 SETTINGS mutations_sync = 2;"

sleep 3

$CLICKHOUSE_CLIENT --query="SELECT count() FROM table(minmax_idx) WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM table(minmax_idx) WHERE i64 = 2 FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="ALTER STREAM minmax_idx MATERIALIZE INDEX idx IN PARTITION 2 SETTINGS mutations_sync = 2"
sleep 3

$CLICKHOUSE_CLIENT --query="SELECT count() FROM table(minmax_idx) WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM table(minmax_idx) WHERE i64 = 2 FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="ALTER STREAM minmax_idx CLEAR INDEX idx IN PARTITION 1 SETTINGS mutations_sync = 2"
$CLICKHOUSE_CLIENT --query="ALTER STREAM minmax_idx CLEAR INDEX idx IN PARTITION 2 SETTINGS mutations_sync = 2"
sleep 3

$CLICKHOUSE_CLIENT --query="SELECT count() FROM table(minmax_idx) WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM table(minmax_idx) WHERE i64 = 2 FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="ALTER STREAM minmax_idx MATERIALIZE INDEX idx SETTINGS mutations_sync = 2"
sleep 3

$CLICKHOUSE_CLIENT --query="SELECT count() FROM table(minmax_idx) WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM table(minmax_idx) WHERE i64 = 2 FORMAT JSON" | grep "rows_read"
sleep 2

$CLICKHOUSE_CLIENT --query="DROP STREAM minmax_idx"
