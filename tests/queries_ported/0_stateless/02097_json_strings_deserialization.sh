#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop stream if exists test_02097" 
$CLICKHOUSE_CLIENT -q "create stream test_02097 (s string, f fixed_string(8)) engine=Memory()"
echo -e "('test\n\t\0\n', 'test\n\t\0\n')" | $CLICKHOUSE_CLIENT -q "insert into test_02097 format Values"
$CLICKHOUSE_CLIENT -q "select * from test_02097 format JSONStringsEachRow" | $CLICKHOUSE_CLIENT -q "insert into test_02097 format JSONStringsEachRow"
$CLICKHOUSE_CLIENT -q "select * from test_02097 format JSONCompactStringsEachRow" | $CLICKHOUSE_CLIENT -q "insert into test_02097 format JSONCompactStringsEachRow"
$CLICKHOUSE_CLIENT -q "select * from test_02097"
$CLICKHOUSE_CLIENT -q "drop stream test_02097" 

