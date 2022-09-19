#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
#  shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS test_02155_csv"

${CLICKHOUSE_CLIENT} --query="create stream test_02155_csv (A int64, S string, D date) Engine=Memory;"


echo "input_format_null_as_default = 1"
cat $CUR_DIR/data_csv/csv_with_slash.csv | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_02155_csv FORMAT CSV SETTINGS input_format_null_as_default = 1"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test_02155_csv"

${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE test_02155_csv"

echo "input_format_null_as_default = 0"
cat $CUR_DIR/data_csv/csv_with_slash.csv | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_02155_csv FORMAT CSV SETTINGS input_format_null_as_default = 0"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test_02155_csv"


${CLICKHOUSE_CLIENT} --query="DROP STREAM test_02155_csv"

