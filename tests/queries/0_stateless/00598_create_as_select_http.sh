#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS test_00598"
$CLICKHOUSE_CURL -sS -d 'create stream test_00598  AS SELECT 1' "$CLICKHOUSE_URL"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test_00598"
$CLICKHOUSE_CLIENT --query="DROP STREAM test_00598"
