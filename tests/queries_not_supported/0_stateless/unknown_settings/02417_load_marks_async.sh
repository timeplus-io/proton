#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} -q "DROP STREAM IF EXISTS test;"
${CLICKHOUSE_CLIENT} -n -q "
CREATE STREAM test
(
n0 uint64,
n1 uint64,
n2 uint64,
n3 uint64,
n4 uint64,
n5 uint64,
n6 uint64,
n7 uint64,
n8 uint64,
n9 uint64
)
ENGINE = MergeTree
ORDER BY n0 SETTINGS min_bytes_for_wide_part = 1;"

${CLICKHOUSE_CLIENT} -q "INSERT INTO test select number, number % 3, number % 5, number % 10, number % 13, number % 15, number % 17, number % 18, number % 22, number % 25 from numbers(1000000)"
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES test"

function test
{
    QUERY_ID=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

    ${CLICKHOUSE_CLIENT} -q "SYSTEM DROP MARK CACHE"
    ${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID}" -q "SELECT * FROM test SETTINGS load_marks_asynchronously=$1 FORMAT Null"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"

    result=$(${CLICKHOUSE_CLIENT} -q "SELECT ProfileEvents['BackgroundLoadingMarksTasks'] FROM system.query_log WHERE query_id = '${QUERY_ID}' AND type = 'QueryFinish' AND current_database = current_database()")
    if [[ $result -ne 0 ]]; then
        echo 'Ok'
    else
        echo 'F'
    fi
}

test 1
