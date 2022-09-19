#!/usr/bin/env bash
# Tags: long

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_HASH="cityHash64(group_array(cityHash64(*)))"

function pack_unpack_compare()
{
    local buf_file
    buf_file="${CLICKHOUSE_TMP}/buf.'.$3"

    ${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS buf_00385"
    ${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS buf_file"

    ${CLICKHOUSE_CLIENT} --query "create stream buf_00385  AS $1"
    local res_orig
    res_orig=$(${CLICKHOUSE_CLIENT} --max_threads=1 --query "SELECT $TABLE_HASH FROM buf_00385")

    ${CLICKHOUSE_CLIENT} --max_threads=1 --query "create stream buf_file ENGINE = File($3) AS SELECT * FROM buf_00385"
    local res_db_file
    res_db_file=$(${CLICKHOUSE_CLIENT} --max_threads=1 --query "SELECT $TABLE_HASH FROM buf_file")

    ${CLICKHOUSE_CLIENT} --max_threads=1 --query "SELECT * FROM buf_00385 FORMAT $3" > "$buf_file"
    local res_ch_local1
    res_ch_local1=$(${CLICKHOUSE_LOCAL} --structure "$2" --file "$buf_file" --table "my super table" --input-format "$3" --output-format TabSeparated --query "SELECT $TABLE_HASH FROM \`my super table\`")
    local res_ch_local2
    res_ch_local2=$(${CLICKHOUSE_LOCAL} --structure "$2" --table "my super table" --input-format "$3" --output-format TabSeparated --query "SELECT $TABLE_HASH FROM \`my super table\`" < "$buf_file")

    ${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS buf_00385"
    ${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS buf_file"
    rm -f "$buf_file" stderr

    echo $((res_orig - res_db_file)) $((res_orig - res_ch_local1)) $((res_orig - res_ch_local2))
}

pack_unpack_compare "SELECT number FROM system.numbers LIMIT 10000" "number uint64" "TabSeparated"
pack_unpack_compare "SELECT number FROM system.numbers LIMIT 10000" "number uint64" "Native"
pack_unpack_compare "SELECT number FROM system.numbers LIMIT 10000" "number uint64" "JSONEachRow"
echo
pack_unpack_compare "SELECT name, is_aggregate FROM system.functions" "name string, is_aggregate uint8" "TabSeparated"
pack_unpack_compare "SELECT name, is_aggregate FROM system.functions" "name string, is_aggregate uint8" "Native"
pack_unpack_compare "SELECT name, is_aggregate FROM system.functions" "name string, is_aggregate uint8" "TSKV"
echo
# Check settings are passed correctly
${CLICKHOUSE_LOCAL} --max_rows_in_distinct=33 -q "SELECT name, value FROM system.settings WHERE name = 'max_rows_in_distinct'"
${CLICKHOUSE_LOCAL} -q "SET max_rows_in_distinct=33; SELECT name, value FROM system.settings WHERE name = 'max_rows_in_distinct'"
${CLICKHOUSE_LOCAL} --max_bytes_before_external_group_by=1 --max_block_size=10 -q "SELECT sum(ignore(*)) FROM (SELECT number, count() FROM numbers(1000) GROUP BY number)"
echo
# Check exta options, we expect zero exit code and no stderr output
(${CLICKHOUSE_LOCAL} --ignore-error --echo -q "SELECT nothing_to_do();SELECT 42;" 2>/dev/null || echo "Wrong RC")
echo
${CLICKHOUSE_LOCAL} -q "create stream sophisticated_default
(
    a uint8 DEFAULT 3,
    b uint8 ALIAS a + 5,
    c uint8
) ;
SELECT count() FROM system.tables WHERE name='sophisticated_default' AND database = currentDatabase();"

# Help is not skipped
[[ $(${CLICKHOUSE_LOCAL} --help | wc -l) -gt 100 ]]
