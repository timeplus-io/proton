#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for format in Native Values JSONCompactEachRow TSKV TSV CSV JSONEachRow JSONCompactEachRow JSONStringsEachRow
do
    echo $format
    ${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS file"
    ${CLICKHOUSE_CLIENT} --query "CREATE STREAM file (x uint64) ENGINE = File($format, '${CLICKHOUSE_DATABASE}/data.$format.lz4') SETTINGS query_mode='table'"
    for size in 10000 100000 1000000 2500000
    do
        ${CLICKHOUSE_CLIENT} --query "TRUNCATE STREAM file SETTINGS query_mode='table'"
        ${CLICKHOUSE_CLIENT} --query "INSERT INTO file SELECT * FROM numbers($size) SETTINGS query_mode='table'"
        ${CLICKHOUSE_CLIENT} --query "SELECT max(x) FROM file SETTINGS query_mode='table'"
    done
done

${CLICKHOUSE_CLIENT} --query "DROP STREAM file"
