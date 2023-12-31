#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "DROP STREAM IF EXISTS enum";
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "create stream enum (x enum8('a' = 1, 'bcdefghijklmno' = 0)) ";
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "INSERT INTO enum VALUES ('a')";
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT * FROM enum";
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "DROP STREAM enum";
