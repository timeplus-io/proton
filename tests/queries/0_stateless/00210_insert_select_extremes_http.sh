#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1" -d @- <<< "DROP STREAM IF EXISTS test_00210"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1" -d @- <<< "create stream test_00210 (x uint8)  "
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1" -d @- <<< "INSERT INTO test_00210 SELECT 1 AS x"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1" -d @- <<< "DROP STREAM test_00210"
