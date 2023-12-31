#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

[ -e "${CLICKHOUSE_TMP}"/test_squashing_block_without_column.out ] && rm "${CLICKHOUSE_TMP}"/test_squashing_block_without_column.out

${CLICKHOUSE_CLIENT} --query "select number as SomeID, number+1 as OtherID from system.numbers limit 1000 into outfile '${CLICKHOUSE_TMP}/test_squashing_block_without_column.out' format Native"

${CLICKHOUSE_CLIENT} --query "drop stream if exists squashed_numbers"
${CLICKHOUSE_CLIENT} --query "create stream squashed_numbers (SomeID uint64, DifferentID uint64, OtherID uint64) engine Memory"

#address=${CLICKHOUSE_HOST}
#port=${CLICKHOUSE_PORT_HTTP}
#url="${CLICKHOUSE_PORT_HTTP_PROTO}://$address:$port/"

${CLICKHOUSE_CURL} -sS --data-binary "@${CLICKHOUSE_TMP}/test_squashing_block_without_column.out" "${CLICKHOUSE_URL}&query=insert%20into%20squashed_numbers%20format%20Native"

${CLICKHOUSE_CLIENT} --query "select 'Still alive'"

${CLICKHOUSE_CLIENT} --query "drop stream squashed_numbers"
