#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d 'SELECT floor(NULL), 1;';
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d 'SELECT to_int64(null), 2';
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d 'SELECT floor(NULL) FORMAT JSONEachRow;';
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d 'SELECT floor(great_circle_distance(NULL, 55.3, 38.7, 55.1)) AS distance format JSONEachRow;';
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d 'SELECT NULL + 1;';
