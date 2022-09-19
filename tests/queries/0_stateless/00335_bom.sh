#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo 'DROP STREAM IF EXISTS bom' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo 'create stream bom (a uint8, b uint8, c uint8) ' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo -ne '1,2,3\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+bom+FORMAT+CSV" --data-binary @-
echo -ne '\xEF\xBB\xBF4,5,6\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+bom+FORMAT+CSV" --data-binary @-
echo 'SELECT * FROM bom ORDER BY a' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo 'DROP STREAM bom' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
