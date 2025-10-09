#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS small_table"

$CLICKHOUSE_CLIENT --query="CREATE STREAM small_table (a uint64 default 0, n uint64) ENGINE = MergeTree() ORDER BY (a) SETTINGS min_bytes_for_wide_part = 0;"

$CLICKHOUSE_CLIENT --query="INSERT INTO small_table (n) SELECT * from system.numbers limit 100000;"
$CLICKHOUSE_CLIENT --query="OPTIMIZE STREAM small_table FINAL;"

cached_query="SELECT count() FROM small_table where n > 0;"

$CLICKHOUSE_CLIENT --use_uncompressed_cache=1 --query="$cached_query" &> /dev/null

$CLICKHOUSE_CLIENT --use_uncompressed_cache=1 --allow_prefetched_read_pool_for_remote_filesystem=0 --allow_prefetched_read_pool_for_local_filesystem=0 --query_id="test-query-uncompressed-cache" --query="$cached_query" &> /dev/null

$CLICKHOUSE_CLIENT --query="SYSTEM FLUSH LOGS"


$CLICKHOUSE_CLIENT --query="SELECT ProfileEvents['Seek'], ProfileEvents['ReadCompressedBytes'], ProfileEvents['UncompressedCacheHits'] AS hit FROM system.query_log WHERE (query_id = 'test-query-uncompressed-cache') and current_database = current_database() AND (type = 2) AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1"

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS small_table"
