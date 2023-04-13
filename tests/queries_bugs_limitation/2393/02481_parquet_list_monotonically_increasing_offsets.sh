#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Parquet"

DATA_FILE=$CUR_DIR/data_parquet/list_monotonically_increasing_offsets.parquet
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS parquet_load"
${CLICKHOUSE_CLIENT} --query="CREATE STREAM parquet_load (list array(int64), json nullable(string)) ENGINE = Memory"
cat "$DATA_FILE" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO parquet_load FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_load" | md5sum
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM parquet_load"
${CLICKHOUSE_CLIENT} --query="drop stream parquet_load"