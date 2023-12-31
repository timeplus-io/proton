#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=none/g')

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS check;"

$CLICKHOUSE_CLIENT --query="CREATE STREAM check (x uint64, y uint64 DEFAULT throw_if(x > 1500000)) ENGINE = Memory;"

seq 1 2000000 | $CLICKHOUSE_CLIENT --query="INSERT INTO check(x) FORMAT TSV" 2>&1 | grep -q "Value passed to 'throw_if' function is non-zero." && echo 'OK' || echo 'FAIL' ||:

$CLICKHOUSE_CLIENT --query="DROP STREAM check;"
