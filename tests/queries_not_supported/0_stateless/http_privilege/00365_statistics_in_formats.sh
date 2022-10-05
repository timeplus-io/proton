#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS numbers";
$CLICKHOUSE_CLIENT --query="create stream numbers (number uint64) engine = MergeTree order by number";
$CLICKHOUSE_CLIENT --query="INSERT INTO numbers select * from system.numbers limit 10";

$CLICKHOUSE_CLIENT --query="SELECT number FROM numbers LIMIT 10 FORMAT JSON" | grep 'rows_read';
$CLICKHOUSE_CLIENT --query="SELECT number FROM numbers LIMIT 10 FORMAT JSONCompact" | grep 'rows_read';
$CLICKHOUSE_CLIENT --query="SELECT number FROM numbers LIMIT 10 FORMAT XML" | grep 'rows_read';

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM numbers LIMIT 10 FORMAT JSON" | grep 'rows_read';
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM numbers LIMIT 10 FORMAT JSONCompact" | grep 'rows_read';
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM numbers LIMIT 10 FORMAT XML" | grep 'rows_read';

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS numbers";
