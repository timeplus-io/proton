#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} --query="drop stream if exists enum_source;"
${CLICKHOUSE_CLIENT} --query="drop stream if exists enum_buf;"

${CLICKHOUSE_CLIENT} --query="create stream enum_source(e Enum8('a'=1)) engine = MergeTree order by tuple()"
${CLICKHOUSE_CLIENT} --query="insert into enum_source values ('a')"
${CLICKHOUSE_CLIENT} --query="create stream enum_buf engine = Log as select * from enum_source;"
${CLICKHOUSE_CLIENT} --query="alter stream enum_source modify column e Enum8('a'=1, 'b'=2);"

${CLICKHOUSE_CLIENT} --query="select * from enum_buf format Native" \
    | ${CLICKHOUSE_CLIENT} --query="insert into enum_source format Native"

${CLICKHOUSE_CLIENT} --query="select * from enum_source;"

${CLICKHOUSE_CLIENT} --query="drop stream enum_source;"
${CLICKHOUSE_CLIENT} --query="drop stream enum_buf;"
