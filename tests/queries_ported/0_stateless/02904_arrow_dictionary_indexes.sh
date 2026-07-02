#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_FILES_PATH=$(${CLICKHOUSE_CLIENT} --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 string')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS arrow_dicts"
${CLICKHOUSE_CLIENT} --query="CREATE STREAM arrow_dicts (a low_cardinality(string)) order by a"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES arrow_dicts"
${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_dicts(a) select to_string(number) from numbers(120);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_dicts(a) select to_string(number) from numbers(120, 120);"
${CLICKHOUSE_CLIENT} --query="SELECT SLEEP(3) FORMAT Null;"
${CLICKHOUSE_CLIENT} --query="SELECT a FROM table(arrow_dicts) FORMAT Arrow SETTINGS output_format_arrow_low_cardinality_as_dictionary=1" > "${USER_FILES_PATH}"/$CLICKHOUSE_TEST_UNIQUE_NAME.arrow

${CLICKHOUSE_CLIENT} --query="DROP STREAM arrow_dicts"

${CLICKHOUSE_CLIENT} -q "select uniq_exact(a) from file('${CLICKHOUSE_TEST_UNIQUE_NAME}.arrow')"

cp -f $CUR_DIR/data_arrow/different_dicts.arrowstream $USER_FILES_PATH/
cp -f $CUR_DIR/data_arrow/non_unique_dict.arrowstream $USER_FILES_PATH/

${CLICKHOUSE_CLIENT} -q "select * from file('different_dicts.arrowstream') order by x"

# FIXME: server crash
# ${CLICKHOUSE_CLIENT} -q "select * from file('non_unique_dict.arrowstream') -- { serverError INCORRECT_DATA }"
