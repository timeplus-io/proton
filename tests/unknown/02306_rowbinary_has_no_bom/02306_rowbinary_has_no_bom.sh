#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "DROP STREAM IF EXISTS stream_with_uint64" | ${CLICKHOUSE_CURL} -d@- -sS "${CLICKHOUSE_URL}"
echo "CREATE STREAM stream_with_uint64(no uint64) ENGINE = MergeTree ORDER BY no" | ${CLICKHOUSE_CURL} -d@- -sS "${CLICKHOUSE_URL}"
echo -en '\xef\xbb\xbf\x00\xab\x3b\xec\x16' | ${CLICKHOUSE_CURL} --data-binary @- "${CLICKHOUSE_URL}&query=INSERT+INTO+table_with_uint64(no)+FORMAT+RowBinary"
echo "SELECT * FROM stream_with_uint64" | ${CLICKHOUSE_CURL} -d@- -sS "${CLICKHOUSE_URL}"
