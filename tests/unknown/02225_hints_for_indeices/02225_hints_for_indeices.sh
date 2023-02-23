#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS t"

$CLICKHOUSE_CLIENT --query="CREATE STREAM t ENGINE=MergeTree ORDER BY n AS SELECT number AS n FROM numbers(10)"

$CLICKHOUSE_CLIENT --query="ALTER STREAM t ADD INDEX test_index n TYPE minmax GRANULARITY 32"

$CLICKHOUSE_CLIENT --query="ALTER STREAM t DROP INDEX test_indes" 2>&1 | grep -q "may be you meant: \['test_index'\]" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --query="ALTER STREAM t ADD INDEX  test_index1 n TYPE minmax GRANULARITY 4 AFTER test_indes" 2>&1 | grep -q "may be you meant: \['test_index'\]" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --query="DROP STREAM t"
