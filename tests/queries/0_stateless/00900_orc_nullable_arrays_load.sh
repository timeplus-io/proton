#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS orc_nullable_arrays"
${CLICKHOUSE_CLIENT} --query="create stream orc_nullable_arrays (arr1 array(nullable(uint32)), arr2 array(nullable(string)), arr3 array(nullable(Decimal(4, 2)))) ENGINE=Memory()"
${CLICKHOUSE_CLIENT} --query="INSERT INTO orc_nullable_arrays VALUES ([1,NULL,2],[NULL,'Some string',NULL],[0.00,NULL,42.42]), ([NULL],[NULL],[NULL]), ([],[],[])"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM orc_nullable_arrays FORMAT ORC" > "${CLICKHOUSE_TMP}"/nullable_arrays.orc

cat "${CLICKHOUSE_TMP}"/nullable_arrays.orc | ${CLICKHOUSE_CLIENT} -q "INSERT INTO orc_nullable_arrays FORMAT ORC"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM orc_nullable_arrays"
${CLICKHOUSE_CLIENT} --query="DROP STREAM orc_nullable_arrays"
