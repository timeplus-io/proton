#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'DROP STREAM IF EXISTS bad_arrays'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'create stream bad_arrays (a array(string)) '
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "INSERT INTO bad_arrays VALUES ([123]), (['123', concat('Hello', ' world!'), to_string(123)])"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT * FROM bad_arrays ORDER BY a'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'DROP STREAM bad_arrays'
