#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS defaults"
$CLICKHOUSE_CLIENT --query="CREATE STREAM defaults (n uint8, s string DEFAULT 'hello') ENGINE = Memory"
echo '{"n": 1} {"n": 2, "s":"world"}' | $CLICKHOUSE_CLIENT --max_insert_block_size=1 --query="INSERT INTO defaults FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --query="SELECT * FROM defaults ORDER BY n"
$CLICKHOUSE_CLIENT --query="DROP STREAM defaults"
