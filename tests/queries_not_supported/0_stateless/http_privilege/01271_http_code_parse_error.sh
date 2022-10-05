#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS test"
${CLICKHOUSE_CLIENT} --query "create stream test (f1 string, f2 string) "

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary 'insert into test (f1, f2) format TSV 1' 2>&1 | grep -F '< HTTP/'

${CLICKHOUSE_CLIENT} --query "DROP STREAM test"
