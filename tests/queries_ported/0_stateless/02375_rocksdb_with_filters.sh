#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS rocksdb_with_filter;"

$CLICKHOUSE_CLIENT --query="CREATE STREAM rocksdb_with_filter (key string, value string) ENGINE=EmbeddedRocksDB PRIMARY KEY key;"
$CLICKHOUSE_CLIENT --query="INSERT INTO rocksdb_with_filter (*) SELECT n.number, n.number*10 FROM numbers(10000) as n;"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM rocksdb_with_filter WHERE key = '5000'"
$CLICKHOUSE_CLIENT --query "SELECT value FROM rocksdb_with_filter WHERE key = '5000' FORMAT JSON" | grep "rows_read" | tr -d "[:blank:]"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM rocksdb_with_filter WHERE key = '5000' OR key = '6000'"
$CLICKHOUSE_CLIENT --query "SELECT value FROM rocksdb_with_filter WHERE key = '5000' OR key = '6000' FORMAT JSON" | grep "rows_read" | tr -d "[:blank:]"

$CLICKHOUSE_CLIENT "--param_key=5000" --query "SELECT count() FROM rocksdb_with_filter WHERE key = {key:string}"
$CLICKHOUSE_CLIENT "--param_key=5000" --query "SELECT value FROM rocksdb_with_filter WHERE key = {key:string} FORMAT JSON" | grep "rows_read" | tr -d "[:blank:]"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM rocksdb_with_filter WHERE key IN ('5000', '6000')"
$CLICKHOUSE_CLIENT --query "SELECT value FROM rocksdb_with_filter WHERE key IN ('5000', '6000') FORMAT JSON" | grep "rows_read" | tr -d "[:blank:]"

$CLICKHOUSE_CLIENT --query="DROP STREAM rocksdb_with_filter;"
