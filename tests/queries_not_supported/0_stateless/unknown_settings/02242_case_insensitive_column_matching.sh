#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Parquet"
DATA_FILE=$CUR_DIR/data_parquet/case_insensitive_column_matching.parquet
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_load"
${CLICKHOUSE_CLIENT} --query="CREATE STREAM parquet_load (iD string, scOre int32) ENGINE = Memory"
cat "$DATA_FILE" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO parquet_load SETTINGS input_format_parquet_case_insensitive_column_matching=true FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_load"
${CLICKHOUSE_CLIENT} --query="drop stream parquet_load"

echo "ORC"
DATA_FILE=$CUR_DIR/data_orc/case_insensitive_column_matching.orc
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS orc_load"
${CLICKHOUSE_CLIENT} --query="CREATE STREAM orc_load (iD string, sCorE int32) ENGINE = Memory"
cat "$DATA_FILE" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO orc_load SETTINGS input_format_orc_case_insensitive_column_matching=true FORMAT ORC"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM orc_load"
${CLICKHOUSE_CLIENT} --query="drop stream orc_load"

echo "Arrow"
DATA_FILE=$CUR_DIR/data_arrow/case_insensitive_column_matching.arrow
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS arrow_load"
${CLICKHOUSE_CLIENT} --query="CREATE STREAM arrow_load (iD string, sCorE int32) ENGINE = Memory"
cat "$DATA_FILE" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO arrow_load SETTINGS input_format_arrow_case_insensitive_column_matching=true FORMAT Arrow"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_load"
${CLICKHOUSE_CLIENT} --query="drop stream arrow_load"
