#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS foo;"
# Missing arguments for array, table not created
$CLICKHOUSE_CLIENT -q "create stream foo (a array) Engine=Memory;" 2&>/dev/null
$CLICKHOUSE_CLIENT -q "SELECT 'Still alive';"
