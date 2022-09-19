#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d "drop stream if exists test_progress"
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d "create stream test_progress (x uint64, y uint64, d date, a array(uint64), s string) engine=MergeTree() order by x"
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d "insert into test_progress select number as x, number + 1 as y, to_date(number) as d, range(number % 10) as a, repeat(to_string(number), 10) as s from numbers(200000)"
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d "SELECT * from test_progress FORMAT JSONEachRowWithProgress" | grep -v --text "progress" | wc -l
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d "drop stream test_progress";
