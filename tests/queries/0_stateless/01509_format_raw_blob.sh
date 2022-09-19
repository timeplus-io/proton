#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -n --query "
DROP STREAM IF EXISTS t;
create stream t (a LowCardinality(Nullable(string))) ;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t FORMAT RawBLOB" < ${BASH_SOURCE[0]}

cat ${BASH_SOURCE[0]} | md5sum
${CLICKHOUSE_CLIENT} -n --query "SELECT * FROM t FORMAT RawBLOB" | md5sum

${CLICKHOUSE_CLIENT} --query "
DROP STREAM t;
"
