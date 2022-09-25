#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS tuples";
${CLICKHOUSE_CLIENT} --query="create stream tuples (t1 tuple(uint32, uint32), t2 tuple(string, string), t3 tuple(tuple(uint32, string), uint32), t4 tuple(tuple(uint32, uint32), tuple(string, string)), t5 tuple(array(uint32), uint32), t6 tuple(tuple(array(uint32), array(uint32)), tuple(array(array(uint32)), uint32)), t7 array(tuple(array(array(uint32)), tuple(array(tuple(uint32, uint32)), uint32)))) ENGINE=Memory()"

${CLICKHOUSE_CLIENT} --query="INSERT INTO tuples VALUES ((1, 2), ('1', '2'), ((1, '1'), 1), ((1, 2), ('1', '2')), ([1,2,3], 1), (([1,2,3], [1,2,3]), ([[1,2,3], [1,2,3]], 1)), [([[1,2,3], [1,2,3]], ([(1, 2), (1, 2)], 1))])"

formats="Arrow Parquet ORC";

for format in ${formats}; do
    echo $format

    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM tuples FORMAT $format" > "${CLICKHOUSE_TMP}"/tuples
    ${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE tuples"
    cat "${CLICKHOUSE_TMP}"/tuples | ${CLICKHOUSE_CLIENT} -q "INSERT INTO tuples FORMAT $format"
    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM tuples"
done

${CLICKHOUSE_CLIENT} --query="DROP STREAM tuples"
