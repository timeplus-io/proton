#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# BOM can be parsed if TSV format has first column that cannot contain arbitrary binary data (such as integer)
# In contrast, BOM cannot be parsed if the first column in string as it can contain arbitrary binary data.

echo 'DROP STREAM IF EXISTS bom' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo 'create stream bom (a uint8, b uint8, c uint8) ' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo -ne '1\t2\t3\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+bom+FORMAT+TSV" --data-binary @-
echo -ne '\xEF\xBB\xBF4\t5\t6\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+bom+FORMAT+TSV" --data-binary @-
echo 'SELECT * FROM bom ORDER BY a' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo 'DROP STREAM bom' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
