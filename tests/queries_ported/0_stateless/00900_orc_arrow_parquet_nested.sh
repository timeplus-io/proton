#!/usr/bin/env bash
# Tags: no-fasttest, no-python

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS nested_table"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS nested_nested_table"

${CLICKHOUSE_CLIENT} --query="CREATE STREAM nested_table (table nested(elem1 int32, elem2 string, elem3 float32)) engine=Memory"

${CLICKHOUSE_CLIENT} --query="CREATE STREAM nested_nested_table (table nested(elem1 int32, elem2 string, elem3 float32, nested nested(elem1 int32, elem2 string, elem3 float32))) engine=Memory"


formats=('Arrow' 'Parquet' 'ORC')
format_files=('arrow' 'parquet' 'orc')

for ((i = 0; i < 3; i++)) do
    echo ${formats[i]}

    ${CLICKHOUSE_CLIENT} --query="TRUNCATE STREAM nested_table"
    cat $CUR_DIR/data_orc_arrow_parquet_nested/nested_table.${format_files[i]} | ${CLICKHOUSE_CLIENT} -q "INSERT INTO nested_table SETTINGS input_format_${format_files[i]}_import_nested = 1 FORMAT ${formats[i]}"

    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM nested_table"


    ${CLICKHOUSE_CLIENT} --query="TRUNCATE STREAM nested_nested_table"
    cat $CUR_DIR/data_orc_arrow_parquet_nested/nested_nested_table.${format_files[i]} | ${CLICKHOUSE_CLIENT} -q "INSERT INTO nested_nested_table SETTINGS input_format_${format_files[i]}_import_nested = 1, input_format_${format_files[i]}_case_insensitive_column_matching = 1  FORMAT ${formats[i]}"

    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM nested_nested_table"
done

${CLICKHOUSE_CLIENT} --query="DROP STREAM nested_table"
${CLICKHOUSE_CLIENT} --query="DROP STREAM nested_nested_table"
