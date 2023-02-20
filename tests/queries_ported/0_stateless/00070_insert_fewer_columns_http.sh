#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo 'DROP STREAM IF EXISTS insert_fewer_columns'                            | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
echo 'CREATE STREAM insert_fewer_columns (a uint8, b uint8) ENGINE = Memory' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
echo 'INSERT INTO insert_fewer_columns (a) VALUES (1), (2)'                 | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
echo 'SELECT * FROM insert_fewer_columns'                                   | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
echo 'DROP STREAM insert_fewer_columns'                                      | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
