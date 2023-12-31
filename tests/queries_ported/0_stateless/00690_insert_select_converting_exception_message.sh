#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS test_00690;"
${CLICKHOUSE_CLIENT} --query "create stream test_00690 (val int64) engine = Memory;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_00690 SELECT 1;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_00690 SELECT NULL AS src;" 2>&1 | grep -oF 'while converting source column src to destination column val';
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_00690 SELECT (number % 2 <> 0) ? 1 : NULL AS src FROM numbers(10);" 2>&1 | grep -oF 'while converting source column src to destination column val';

${CLICKHOUSE_CLIENT} --query "DROP STREAM test_00690;"
