#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS t"

$CLICKHOUSE_CLIENT --query="create stream t (x int32, y int32, projection pToDrop (select x, y order by x)) engine = MergeTree order by y;"

$CLICKHOUSE_CLIENT --query="ALTER STREAM t DROP PROJECTION pToDro" 2>&1 | grep -q "Maybe you meant: \['pToDrop'\]" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --query="DROP STREAM t"
