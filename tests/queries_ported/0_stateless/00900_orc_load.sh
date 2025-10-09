#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest, no-python

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS orc_load"
${CLICKHOUSE_CLIENT} --query="CREATE STREAM orc_load (int int32, smallint int8, bigint int64, float float32, double float64, date date, y string, datetime64 datetime64(3)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="insert into orc_load values (0, 0, 0, 0, 0, '2019-01-01', 'test1', to_datetime64('2019-01-01 02:03:04.567', 3)), (2147483647, -1, 9223372036854775806, 123.345345, 345345.3453451212, '2019-01-01', 'test2', to_datetime64('2019-01-01 02:03:04.567', 3))"
${CLICKHOUSE_CLIENT} --query="select * from orc_load FORMAT ORC" > "${CLICKHOUSE_TMP}"/test.orc
${CLICKHOUSE_CLIENT} --query="truncate stream orc_load"

cat "${CLICKHOUSE_TMP}"/test.orc | ${CLICKHOUSE_CLIENT} -q "insert into orc_load format ORC"
${CLICKHOUSE_CLIENT} -q "insert into orc_load format ORC" < "${CLICKHOUSE_TMP}"/test.orc
${CLICKHOUSE_CLIENT} --query="select * from orc_load"
${CLICKHOUSE_CLIENT} --query="drop stream orc_load"
