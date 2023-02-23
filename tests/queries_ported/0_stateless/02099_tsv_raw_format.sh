#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS test_02099"
$CLICKHOUSE_CLIENT -q "CREATE STREAM test_02099 (number uint64, string string, date Date) ENGINE=Memory()"

FORMATS=('TSVRaw' 'TSVRawWithNames' 'TSVRawWithNamesAndTypes' 'TabSeparatedRaw'  'TabSeparatedRawWithNames'  'TabSeparatedRawWithNamesAndTypes')

for format in "${FORMATS[@]}"
do
    echo $format
    $CLICKHOUSE_CLIENT -q "INSERT INTO test_02099 SELECT number, to_string(number), to_date(number) FROM numbers(3)"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM test_02099 FORMAT $format"
        
    $CLICKHOUSE_CLIENT -q "SELECT * FROM test_02099 FORMAT $format" | $CLICKHOUSE_CLIENT -q "INSERT INTO test_02099 FORMAT $format"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM test_02099"

    $CLICKHOUSE_CLIENT -q "TRUNCATE STREAM test_02099"
done

$CLICKHOUSE_CLIENT -q "DROP STREAM test_02099"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS test_nullable_02099"
$CLICKHOUSE_CLIENT -q "CREATE STREAM test_nullable_02099 ENGINE=Memory() AS SELECT number % 2 ? NULL : number from numbers(4)";

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_02099 FORMAT TSVRaw"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_02099 FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -q "INSERT INTO test_nullable_02099 FORMAT TSVRaw"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_02099"


$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_02099 FORMAT TSV" | $CLICKHOUSE_CLIENT -q "INSERT INTO test_nullable_02099 FORMAT TSVRaw"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_02099 FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -q "INSERT INTO test_nullable_02099 FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_02099"

$CLICKHOUSE_CLIENT -q "DROP STREAM test_nullable_02099"


$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS test_nullable_string_02099"
$CLICKHOUSE_CLIENT -q "CREATE STREAM test_nullable_string_02099 (s nullable(string)) ENGINE=Memory()";

echo 'nSome text' | $CLICKHOUSE_CLIENT -q "INSERT INTO test_nullable_string_02099 FORMAT TSVRaw"

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_string_02099"
$CLICKHOUSE_CLIENT -q "DROP STREAM test_nullable_string_02099"


$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS test_parallel_parsing_02099"
$CLICKHOUSE_CLIENT -q "CREATE STREAM test_parallel_parsing_02099 (x uint64, a array(uint64), s string) ENGINE=Memory()";
$CLICKHOUSE_CLIENT -q "SELECT number AS x, range(number % 50) AS a, to_string(a) AS s FROM numbers(1000000) FORMAT TSVRaw" |  $CLICKHOUSE_CLIENT --input_format_parallel_parsing=0 -q "INSERT INTO test_parallel_parsing_02099 FORMAT TSVRaw"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_parallel_parsing_02099 ORDER BY x" | md5sum

$CLICKHOUSE_CLIENT -q "TRUNCATE STREAM test_parallel_parsing_02099"

$CLICKHOUSE_CLIENT -q "SELECT number AS x, range(number % 50) AS a, to_string(a) AS s FROM numbers(1000000) FORMAT TSVRaw" |  $CLICKHOUSE_CLIENT --input_format_parallel_parsing=1 -q "INSERT INTO test_parallel_parsing_02099 FORMAT TSVRaw"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_parallel_parsing_02099 ORDER BY x" | md5sum

$CLICKHOUSE_CLIENT -q "DROP STREAM test_parallel_parsing_02099"

