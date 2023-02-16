#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


FILE_NAME="${CLICKHOUSE_DATABASE}_test.native.zstd"

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS test"

${CLICKHOUSE_CLIENT} --query "CREATE STREAM test (a int64, b int64 MATERIALIZED a) ENGINE = MergeTree() PRIMARY KEY tuple()"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test VALUES (1)"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test INTO OUTFILE '${CLICKHOUSE_TMP}/${FILE_NAME}' FORMAT Native"

${CLICKHOUSE_CLIENT} --query "TRUNCATE STREAM test"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test FROM INFILE '${CLICKHOUSE_TMP}/${FILE_NAME}'"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test"

${CLICKHOUSE_CLIENT} --query "DROP STREAM test"

rm -f "${CLICKHOUSE_TMP}/${FILE_NAME}"
