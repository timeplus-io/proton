#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

table="optimize_me_finally"
name="$CLICKHOUSE_DATABASE.$table"
res_rows=150000 # >= vertical_merge_algorithm_min_rows_to_activate

function get_num_parts {
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE active AND database='$CLICKHOUSE_DATABASE' AND table='$table'"
}

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS $name"

$CLICKHOUSE_CLIENT -q "create stream $name (
date date,
Sign int8,
ki uint64,

ds string,
di01 uint64,
di02 uint64,
di03 uint64,
di04 uint64,
di05 uint64,
di06 uint64,
di07 uint64,
di08 uint64,
di09 uint64,
di10 uint64,
n nested(
i uint64,
s string
)
)
ENGINE = CollapsingMergeTree(date, (date, ki), 8192, Sign)"

$CLICKHOUSE_CLIENT -q "INSERT INTO $name (date, Sign, ki) SELECT
to_date(0) AS date,
toInt8(1) AS Sign,
to_uint64(0) AS ki
FROM system.numbers LIMIT 9000" --server_logs_file=/dev/null

$CLICKHOUSE_CLIENT -q "INSERT INTO $name (date, Sign, ki) SELECT
to_date(0) AS date,
toInt8(1) AS Sign,
number AS ki
FROM system.numbers LIMIT 9000, 9000" --server_logs_file=/dev/null

$CLICKHOUSE_CLIENT -q "INSERT INTO $name SELECT
to_date(0) AS date,
toInt8(1) AS Sign,
number AS ki,
hex(number) AS ds,
number AS di01,
number AS di02,
number AS di03,
number AS di04,
number AS di05,
number AS di06,
number AS di07,
number AS di08,
number AS di09,
number AS di10,
[number, number+1] AS \`n.i\`,
[hex(number), hex(number+1)] AS \`n.s\`
FROM system.numbers LIMIT $res_rows" --server_logs_file=/dev/null

while [[ $(get_num_parts) -ne 1 ]] ; do $CLICKHOUSE_CLIENT -q "OPTIMIZE STREAM $name PARTITION 197001" --server_logs_file=/dev/null; done

$CLICKHOUSE_CLIENT -q "ALTER STREAM $name ADD COLUMN n.a array(string)"
$CLICKHOUSE_CLIENT -q "ALTER STREAM $name ADD COLUMN da array(string) DEFAULT ['def']"

$CLICKHOUSE_CLIENT -q "OPTIMIZE STREAM $name PARTITION 197001 FINAL" --server_logs_file=/dev/null

$CLICKHOUSE_CLIENT -q "ALTER STREAM $name MODIFY COLUMN n.a array(string) DEFAULT ['zzz']"
$CLICKHOUSE_CLIENT -q "ALTER STREAM $name MODIFY COLUMN da array(string) DEFAULT ['zzz']"

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(Sign), sum(ki = di05), sum(hex(ki) = ds), sum(ki = n.i[1]), sum([hex(ki), hex(ki+1)] = n.s) FROM $name"
$CLICKHOUSE_CLIENT -q "SELECT groupUniqArray(da), groupUniqArray(n.a) FROM $name"

hash_src=$($CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT cityHash64(group_array(ki)) FROM $name")
hash_ref=$($CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT cityHash64(group_array(ki)) FROM (SELECT number as ki FROM system.numbers LIMIT $res_rows)")
echo $(( $hash_src - $hash_ref ))

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS $name"
