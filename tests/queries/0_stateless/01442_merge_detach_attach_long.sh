#!/usr/bin/env bash
# Tags: long, no-parallel

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=none/g')

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS t"
${CLICKHOUSE_CLIENT} --query="create stream t (x int8) ENGINE = MergeTree ORDER BY tuple()"

for _ in {1..100}; do
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO t VALUES (0)"
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO t VALUES (0)"
    ${CLICKHOUSE_CLIENT} --query="OPTIMIZE STREAM t FINAL" 2>/dev/null &
    ${CLICKHOUSE_CLIENT} --query="ALTER STREAM t DETACH PARTITION tuple()"
    ${CLICKHOUSE_CLIENT} --query="SELECT count() FROM t HAVING count() > 0"
done

wait

$CLICKHOUSE_CLIENT -q "DROP STREAM t"
