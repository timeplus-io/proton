#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-backward-compatibility-check

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

$CLICKHOUSE_CLIENT -q "insert into stream function file(data.jsonl, 'JSONEachRow', 'x uint32 default 42, y string') select number as x, 'string' as y from numbers(10)"

$CLICKHOUSE_CLIENT -q "drop stream if exists test"
$CLICKHOUSE_CLIENT -q "create stream test engine=File(JSONEachRow, 'data.jsonl')"
$CLICKHOUSE_CLIENT -q "show create test"
$CLICKHOUSE_CLIENT -q "detach stream test"

rm $USER_FILES_PATH/data.jsonl

$CLICKHOUSE_CLIENT -q "attach stream test"
$CLICKHOUSE_CLIENT -q "select * from test" 2>&1 | grep -q "FILE_DOESNT_EXIST" && echo "OK" || echo "FAIL"


$CLICKHOUSE_CLIENT -q "drop stream test"
$CLICKHOUSE_CLIENT -q "create stream test (x uint64) engine=Memory()"

$CLICKHOUSE_CLIENT -q "drop stream if exists test_dist"
$CLICKHOUSE_CLIENT -q "create stream test_dist engine=Distributed('test_shard_localhost', current_database(), 'test')"

$CLICKHOUSE_CLIENT -q "detach stream test_dist"
$CLICKHOUSE_CLIENT -q "drop stream test"
$CLICKHOUSE_CLIENT -q "attach stream test_dist"
$CLICKHOUSE_CLIENT --prefer_localhost_replica=1 -q "select * from test_dist" 2>&1 | grep -q "UNKNOWN_TABLE" && echo "OK" || echo "FAIL"

