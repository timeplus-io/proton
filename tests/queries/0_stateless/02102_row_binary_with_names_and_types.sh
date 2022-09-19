#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS test_02102"
$CLICKHOUSE_CLIENT -q "create stream test_02102 (x uint32, y string DEFAULT 'default', z date) engine=Memory()"



$CLICKHOUSE_CLIENT -q "SELECT to_uint32(1) AS x, 'text' AS y, to_date('2020-01-01') AS z FORMAT RowBinaryWithNames" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"

$CLICKHOUSE_CLIENT -q "SELECT to_uint32(1) AS x, 'text' AS y, to_date('2020-01-01') AS z FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"


$CLICKHOUSE_CLIENT -q "SELECT to_uint32(1) AS x, 'text' AS y, to_date('2020-01-01') AS z FORMAT RowBinaryWithNames" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=0 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"

$CLICKHOUSE_CLIENT -q "SELECT to_uint32(1) AS x, 'text' AS y, to_date('2020-01-01') AS z FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=0 --input_format_with_types_use_header=0 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"


$CLICKHOUSE_CLIENT -q "SELECT 'text' AS y, to_date('2020-01-01') AS z, to_uint32(1) AS x FORMAT RowBinaryWithNames" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"

$CLICKHOUSE_CLIENT -q "SELECT 'text' AS y, to_date('2020-01-01') AS z, to_uint32(1) AS x FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"


$CLICKHOUSE_CLIENT -q "SELECT to_uint32(1) AS x FORMAT RowBinaryWithNames" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"

$CLICKHOUSE_CLIENT -q "SELECT to_uint32(1) AS x FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"


$CLICKHOUSE_CLIENT -q "SELECT to_uint32(1) AS x FORMAT RowBinaryWithNames" | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=0 --input_format_with_names_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"

$CLICKHOUSE_CLIENT -q "SELECT to_uint32(1) AS x FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=0 --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"


$CLICKHOUSE_CLIENT -q "SELECT to_uint32(1) AS x, [[1, 2, 3], [4, 5], []] as a FORMAT RowBinaryWithNames" | $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 --input_format_with_names_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNames"  2>&1 | grep -F -q "CANNOT_SKIP_UNKNOWN_FIELD" && echo 'OK' || echo 'FAIL'


$CLICKHOUSE_CLIENT -q "SELECT to_uint32(1) AS x, [[1, 2, 3], [4, 5], []] as a FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"


$CLICKHOUSE_CLIENT -q "SELECT 'text' AS x, to_date('2020-01-01') AS y, to_uint32(1) AS z FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -q "SELECT to_uint32(1) AS x, 'text' as z, to_date('2020-01-01') AS y FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -q "DROP STREAM test_02102"

