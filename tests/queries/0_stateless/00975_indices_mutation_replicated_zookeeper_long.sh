#!/usr/bin/env bash
# Tags: long, replica

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS indices_mutaions1;"
$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS indices_mutaions2;"


$CLICKHOUSE_CLIENT -n --query="
create stream indices_mutaions1
(
    u64 uint64,
    i64 int64,
    i32 int32,
    INDEX idx (i64, u64 * i64) TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/indices_mutaions', 'r1')
PARTITION BY i32
ORDER BY u64
SETTINGS index_granularity = 2;

create stream indices_mutaions2
(
    u64 uint64,
    i64 int64,
    i32 int32,
    INDEX idx (i64, u64 * i64) TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/indices_mutaions', 'r2')
PARTITION BY i32
ORDER BY u64
SETTINGS index_granularity = 2;"


$CLICKHOUSE_CLIENT --query="INSERT INTO indices_mutaions1 VALUES
(0, 2, 1),
(1, 1, 1),
(2, 1, 1),
(3, 1, 1),
(4, 1, 1),
(5, 2, 1),
(6, 1, 2),
(7, 1, 2),
(8, 1, 2),
(9, 1, 2)"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA indices_mutaions2"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM indices_mutaions2 WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM indices_mutaions2 WHERE i64 = 2 FORMAT JSON;" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="ALTER STREAM indices_mutaions1 CLEAR INDEX idx IN PARTITION 1;" --replication_alter_partitions_sync=2 --mutations_sync=2

$CLICKHOUSE_CLIENT --query="SELECT count() FROM indices_mutaions2 WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM indices_mutaions2 WHERE i64 = 2 FORMAT JSON;" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="ALTER STREAM indices_mutaions1 MATERIALIZE INDEX idx IN PARTITION 1;" --replication_alter_partitions_sync=2 --mutations_sync=2

$CLICKHOUSE_CLIENT --query="SELECT count() FROM indices_mutaions2 WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM indices_mutaions2 WHERE i64 = 2 FORMAT JSON;" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="DROP STREAM indices_mutaions1"
$CLICKHOUSE_CLIENT --query="DROP STREAM indices_mutaions2"
