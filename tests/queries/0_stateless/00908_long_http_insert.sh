#!/usr/bin/env bash
# Tags: long

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo 'DROP STREAM IF EXISTS table_for_insert'                            | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
echo 'create stream table_for_insert (a uint8, b uint8) ' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
echo "INSERT INTO table_for_insert VALUES $(printf '%*s' "1000000" "" | sed 's/ /(1, 2)/g')" | ${CLICKHOUSE_CURL_COMMAND} -q --max-time 30 -sSg "${CLICKHOUSE_URL}" -d @-
echo 'SELECT count(*) FROM table_for_insert'                            | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
echo 'DROP STREAM IF EXISTS table_for_insert'                            | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
