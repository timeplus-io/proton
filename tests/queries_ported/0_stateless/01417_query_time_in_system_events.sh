#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_BEFORE=`${CLICKHOUSE_CLIENT} --query="SELECT event,value FROM system.events WHERE event IN ('QueryTimeMicroseconds','SelectQueryTimeMicroseconds') FORMAT CSV"`

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS test"
${CLICKHOUSE_CLIENT} --query="CREATE STREAM test (k uint32) ENGINE=MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test (k) SELECT sleep(1)"
${CLICKHOUSE_CLIENT} --query="SELECT sleep(1)" > /dev/null
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS test"

DATA_AFTER=`${CLICKHOUSE_CLIENT} --query="SELECT event,value FROM system.events WHERE event IN ('QueryTimeMicroseconds','SelectQueryTimeMicroseconds') FORMAT CSV"`

declare -A VALUES_BEFORE
VALUES_BEFORE=(["\"QueryTimeMicroseconds\""]="0" ["\"SelectQueryTimeMicroseconds\""]="0")
declare -A VALUES_AFTER
VALUES_AFTER=(["\"QueryTimeMicroseconds\""]="0" ["\"SelectQueryTimeMicroseconds\""]="0")

for RES in ${DATA_BEFORE}
do
    IFS=',' read -ra FIELDS <<< ${RES}
    VALUES_BEFORE[${FIELDS[0]}]=${FIELDS[1]}
done

for RES in ${DATA_AFTER}
do
    IFS=',' read -ra FIELDS <<< ${RES}
    VALUES_AFTER[${FIELDS[0]}]=${FIELDS[1]}
done

let QUERY_TIME=${VALUES_AFTER[\"QueryTimeMicroseconds\"]}-${VALUES_BEFORE[\"QueryTimeMicroseconds\"]}
let SELECT_QUERY_TIME=${VALUES_AFTER[\"SelectQueryTimeMicroseconds\"]}-${VALUES_BEFORE[\"SelectQueryTimeMicroseconds\"]}

if [[ "${QUERY_TIME}" -lt "2000000" ]]; then
    echo "QueryTimeMicroseconds: Fail (${QUERY_TIME})"
else
    echo "QueryTimeMicroseconds: Ok"
fi
if [[ "${SELECT_QUERY_TIME}" -lt "1000000" ]]; then
    echo "SelectQueryTimeMicroseconds: Fail (${SELECT_QUERY_TIME})"
else
    echo "SelectQueryTimeMicroseconds: Ok"
fi
