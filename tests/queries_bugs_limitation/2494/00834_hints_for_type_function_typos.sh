#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "select c23ount(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['count'" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select cunt(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['count'" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select positin(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['position'" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select POSITIO(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['position'" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select fount(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['count'" | grep "Maybe you meant: \['round'" | grep "Or unknown aggregate function" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select positin(*) from system.functions;" 2>&1 | grep -v "Or unknown aggregate function" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select pov(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['pow'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select getColumnStructure('abc');" 2>&1 | grep "Maybe you meant: \['dump_column_structure'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select gutColumnStructure('abc');" 2>&1 | grep "Maybe you meant: \['dump_column_structure'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select gupColumnStructure('abc');" 2>&1 | grep "Maybe you meant: \['dump_column_structure'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select provideColumnStructure('abc');" 2>&1 | grep "Maybe you meant: \['dump_column_structure'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select multisearchallposicionutf7('abc');" 2>&1 | grep "Maybe you meant: \['multi_search_all_positions_utf8','multi_search_all_positions'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select multisearchallposicionutf7casesensitive('abc');" 2>&1 | grep "Maybe you meant: \['multi_search_all_positions_case_insensitive']" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select multiSearchAllPosicionSensitiveUTF8('abc');" 2>&1 | grep "Maybe you meant: \['multi_search_any_case_insensitive_utf8','multi_search_all_positions_utf8'\]." &>/dev/null;

$CLICKHOUSE_CLIENT -q "select * FROM numberss(10);" 2>&1 | grep "Maybe you meant: \['numbers'\,'numbers_mt'\]." &>/dev/null
$CLICKHOUSE_CLIENT -q "select * FROM anothernumbers(10);" 2>&1 | grep -v "Maybe you meant: \['numbers'\,'numbers_mt'\]." &>/dev/null
$CLICKHOUSE_CLIENT -q "select * FROM mynumbers(10);" 2>&1 | grep "Maybe you meant: \['numbers'\]." &>/dev/null

$CLICKHOUSE_CLIENT -q "CREATE STREAM stored_aggregates (d Date, Uniq aggregate_function(uniq, uint64)) ENGINE = MergeTre(d, d, 8192);" 2>&1 | grep "Maybe you meant: \['MergeTree'\]." &>/dev/null
$CLICKHOUSE_CLIENT -q "CREATE STREAM stored_aggregates (d Date, Uniq AgregateFunction(uniq, uint64)) ENGINE = MergeTree(d, d, 8192);" 2>&1 | grep "Maybe you meant: \['aggregate_function'\]." &>/dev/null
