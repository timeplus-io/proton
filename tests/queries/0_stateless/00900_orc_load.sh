#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_orc/test.orc

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS orc_load"
${CLICKHOUSE_CLIENT} --query="create stream orc_load (int int32, smallint int8, bigint int64, float Float32, double float64, date date, y string) "
cat "$DATA_FILE"  | ${CLICKHOUSE_CLIENT} -q "insert into orc_load format ORC"
timeout 3 ${CLICKHOUSE_CLIENT} -q "insert into orc_load format ORC" < $DATA_FILE
${CLICKHOUSE_CLIENT} --query="select * from orc_load"

${CLICKHOUSE_CLIENT} --query="drop stream orc_load"
