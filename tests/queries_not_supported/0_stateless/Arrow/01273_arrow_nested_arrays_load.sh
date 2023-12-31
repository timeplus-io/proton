#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS arrow_nested_arrays"
${CLICKHOUSE_CLIENT} --query="CREATE STREAM arrow_nested_arrays (arr1 array(array(array(uint32))), arr2 array(array(array(string))), arr3 array(array(nullable(uint32))), arr4 array(array(nullable(string)))) engine=Memory()"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_nested_arrays VALUES ([[[1,2,3],[1,2,3]],[[1,2,3]],[[],[1,2,3]]],[[['Some string','Some string'],[]],[['Some string']],[[]]],[[NULL,1,2],[NULL],[1,2],[]],[['Some string',NULL,'Some string'],[NULL],[]])"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_nested_arrays FORMAT Arrow" > "${CLICKHOUSE_TMP}"/nested_arrays.arrow

cat "${CLICKHOUSE_TMP}"/nested_arrays.arrow | ${CLICKHOUSE_CLIENT} -q "INSERT INTO arrow_nested_arrays FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_nested_arrays"
${CLICKHOUSE_CLIENT} --query="DROP stream arrow_nested_arrays"
