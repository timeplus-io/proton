#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: create user

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "drop stream if exists test_table"
$CLICKHOUSE_CLIENT --query "create stream test_table
(
    a      uint16 DEFAULT 0,
    c      low_cardinality(string) DEFAULT '',
    t_date low_cardinality(string) DEFAULT '',
    ex     low_cardinality(string) DEFAULT '',
    team   low_cardinality(string) DEFAULT '',
    g      low_cardinality(string) DEFAULT '',
    mt     fixed_string(1) DEFAULT ' ',
    rw_ts  int64 DEFAULT 0,
    exr_t  int64 DEFAULT 0,
    en     uint16 DEFAULT 0,
    f_t    int64 DEFAULT 0,
    j      uint64 DEFAULT 0,
    oj     uint64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY (c, t_date)
ORDER BY (ex, team, g, mt, rw_ts, exr_t, en, f_t, j, oj)
SETTINGS index_granularity = 8192"

$CLICKHOUSE_CLIENT --query "
INSERT INTO test_table(t_date, c,team, a) SELECT
array_join([to_date('2021-07-15'),to_date('2021-07-16')]) as t_date,
array_join(['aur','rua']) as c,
array_join(['AWD','ZZZ']) as team,
array_join([3183,3106,0,3130,3108,3126,3109,3107,3182,3180,3129,3128,3125,3266]) as a
FROM numbers(600);"

$CLICKHOUSE_CLIENT --query "DROP ROLE IF exists AWD;"
$CLICKHOUSE_CLIENT --query "create role AWD;"
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON *.* FROM AWD;"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS AWD_user;"
$CLICKHOUSE_CLIENT --query "CREATE USER AWD_user IDENTIFIED WITH plaintext_password BY 'AWD_pwd' DEFAULT ROLE AWD;"

$CLICKHOUSE_CLIENT --query "GRANT SELECT ON test_table TO AWD;"

$CLICKHOUSE_CLIENT --query "DROP ROW POLICY IF EXISTS ttt_bu_test_table_AWD ON test_table;"
$CLICKHOUSE_CLIENT --query "CREATE ROW POLICY ttt_bu_test_table_AWD ON test_table FOR SELECT USING team = 'AWD' TO AWD;"

$CLICKHOUSE_CLIENT  --user=AWD_user --password=AWD_pwd  --query "
SELECT count() AS count
 FROM test_table
WHERE
 t_date = '2021-07-15' AND c = 'aur' AND a=3130;
"

$CLICKHOUSE_CLIENT  --user=AWD_user --password=AWD_pwd  --query "
SELECT
    team,
    a,
    t_date,
    count() AS count
FROM test_table
WHERE (t_date = '2021-07-15') AND (c = 'aur') AND (a = 3130)
GROUP BY
    team,
    a,
    t_date;
"

$CLICKHOUSE_CLIENT  --user=AWD_user --password=AWD_pwd  --query "
SELECT count() AS count
FROM test_table
WHERE (t_date = '2021-07-15') AND (c = 'aur') AND (a = 313)
"
