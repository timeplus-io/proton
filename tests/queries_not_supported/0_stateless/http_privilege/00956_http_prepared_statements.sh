#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "DROP STREAM IF EXISTS ps";
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "create stream ps (i uint8, s string, d date) ";

${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "INSERT INTO ps VALUES (1, 'Hello, world', '2005-05-05')";
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "INSERT INTO ps VALUES (2, 'test', '2019-05-25')";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_id=1" \
    -d "SELECT * FROM ps WHERE i = {id:uint8} ORDER BY i, s, d";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_phrase=Hello,+world" \
    -d "SELECT * FROM ps WHERE s = {phrase:string} ORDER BY i, s, d";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_date=2019-05-25" \
    -d "SELECT * FROM ps WHERE d = {date:date} ORDER BY i, s, d";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_id=2&param_phrase=test" \
    -d "SELECT * FROM ps WHERE i = {id:uint8} and s = {phrase:string} ORDER BY i, s, d";

${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "DROP STREAM ps";
