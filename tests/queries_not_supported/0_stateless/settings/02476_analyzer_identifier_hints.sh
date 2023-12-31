#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
    DROP STREAM IF EXISTS test_table;
    CREATE STREAM test_table
    (
        id uint64,
        value string
    ) ENGINE=MergeTree ORDER BY id;

    INSERT INTO test_table VALUES (0, 'Value');
";

$CLICKHOUSE_CLIENT -q "SELECT value_ FROM test_table SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_table.value_ FROM test_table SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_tabl.value_ FROM test_table SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_table.value_ FROM test_table AS test_table_alias SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_tabl.value_ FROM test_table AS test_table_alias SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_table_alias.value_ FROM test_table AS test_table_alias SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table_alias.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_table_alia.value_ FROM test_table AS test_table_alias SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table_alias.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT value_ FROM (SELECT 1 AS value) SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT subquery.value_ FROM (SELECT 1 AS value) AS subquery SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['subquery.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT subquer.value_ FROM (SELECT 1 AS value) AS subquery SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['subquery.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "WITH cte_subquery AS (SELECT 1 AS value) SELECT value_ FROM cte_subquery SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "WITH cte_subquery AS (SELECT 1 AS value) SELECT cte_subquery.value_ FROM cte_subquery SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['cte_subquery.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "WITH cte_subquery AS (SELECT 1 AS value) SELECT cte_subquer.value_ FROM cte_subquery SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['cte_subquery.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "WITH cte_subquery AS (SELECT 1 AS value) SELECT cte_subquery_alias.value_ FROM cte_subquery AS cte_subquery_alias SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['cte_subquery_alias.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "WITH cte_subquery AS (SELECT 1 AS value) SELECT cte_subquery_alia.value_ FROM cte_subquery AS cte_subquery_alias SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['cte_subquery_alias.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT 1 AS constant_value, constant_valu SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['constant_value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT 1 AS constant_value, constant_valu SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['constant_value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT 1 AS constant_value, array_map(lambda_argument -> lambda_argument + constant_valu, [1, 2, 3]) SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['constant_value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "WITH 1 AS constant_value SELECT (SELECT constant_valu) SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['constant_value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -n -q "
    DROP STREAM IF EXISTS test_table_compound;
    CREATE STREAM test_table_compound
    (
        id uint64,
        value tuple(value_1 string)
    ) ENGINE=MergeTree ORDER BY id;

    INSERT INTO test_table_compound VALUES (0, tuple('Value_1'));
";

$CLICKHOUSE_CLIENT -q "SELECT value.value_ FROM test_table_compound SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_table_compound.value.value_ FROM test_table_compound SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table_compound.value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_tabl_compound.value.value_ FROM test_table_compound SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table_compound.value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_table_compound.value.value_ FROM test_table_compound AS test_table_compound_alias SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table_compound.value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_tabl_compound.value.value_ FROM test_table_compound AS test_table_compound_alias SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table_compound.value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_table_compound_alias.value.value_ FROM test_table_compound AS test_table_compound_alias SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table_compound_alias.value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_table_compound_alia.value.value_ FROM test_table_compound AS test_table_compound_alias SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table_compound_alias.value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT value.value_ FROM (SELECT cast(tuple(1), 'tuple(value_1 string)') AS value) SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT subquery.value.value_ FROM (SELECT cast(tuple(1), 'tuple(value_1 string)') AS value) AS subquery SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['subquery.value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT subquer.value.value_ FROM (SELECT cast(tuple(1), 'tuple(value_1 string)') AS value) AS subquery SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['subquery.value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "WITH cte_subquery AS (SELECT cast(tuple(1), 'tuple(value_1 string)') AS value) SELECT value.value_ FROM cte_subquery SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "WITH cte_subquery AS (SELECT cast(tuple(1), 'tuple(value_1 string)') AS value) SELECT cte_subquery.value.value_ FROM cte_subquery SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['cte_subquery.value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "WITH cte_subquery AS (SELECT cast(tuple(1), 'tuple(value_1 string)') AS value) SELECT cte_subquer.value.value_ FROM cte_subquery SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['cte_subquery.value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "WITH cte_subquery AS (SELECT cast(tuple(1), 'tuple(value_1 string)') AS value) SELECT cte_subquery_alias.value.value_ FROM cte_subquery AS cte_subquery_alias SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['cte_subquery_alias.value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "WITH cte_subquery AS (SELECT cast(tuple(1), 'tuple(value_1 string)') AS value) SELECT cte_subquery_alia.value.value_ FROM cte_subquery AS cte_subquery_alias SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['cte_subquery_alias.value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT cast(tuple(1), 'tuple(value_1 string)') AS constant_value, constant_value.value_ SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['constant_value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT cast(tuple(1), 'tuple(value_1 string)') AS constant_value, constant_valu.value_ SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['constant_value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT cast(tuple(1), 'tuple(value_1 string)') AS constant_value, array_map(lambda_argument -> lambda_argument + constant_value.value_, [1, 2, 3]) SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['constant_value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "WITH cast(tuple(1), 'tuple(value_1 string)') AS constant_value SELECT (SELECT constant_value.value_) SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['constant_value.value_1'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -n -q "
    DROP STREAM IF EXISTS test_table_1;
    CREATE STREAM test_table_1
    (
        id uint64,
        value string
    ) ENGINE=MergeTree ORDER BY id;

    INSERT INTO test_table_1 VALUES (0, 'Value');

    DROP STREAM IF EXISTS test_table_2;
    CREATE STREAM test_table_2
    (
        id uint64,
        value string
    ) ENGINE=MergeTree ORDER BY id;

    INSERT INTO test_table_2 VALUES (0, 'Value');
";

$CLICKHOUSE_CLIENT -q "SELECT test_table_1.value_ FROM test_table_1 INNER JOIN test_table_2 ON test_table_1.id = test_table_2.id SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table_1.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT test_table_2.value_ FROM test_table_1 INNER JOIN test_table_2 ON test_table_1.id = test_table_2.id SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_table_2.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT t1.value_ FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 ON t1.id = t2.id SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['t1.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT t2.value_ FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 ON t1.id = t2.id SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['t2.value'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT 1";

$CLICKHOUSE_CLIENT -n -q "
    DROP STREAM test_table;
    DROP STREAM test_table_compound;
    DROP STREAM test_table_1;
    DROP STREAM test_table_2;
";
