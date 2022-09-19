#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


for format in TSV TabSeparated TSVWithNames TSVWithNamesAndTypes CSV Parquet ORC Arrow JSONEachRow JSONCompactEachRow CustomSeparatedWithNamesAndTypes
do
    $CLICKHOUSE_CLIENT -q "insert into table function file('test_02167.$format', 'auto', 'x uint64') select * from numbers(2)"
    $CLICKHOUSE_CLIENT -q "select * from file('test_02167.$format')"
    $CLICKHOUSE_CLIENT -q "select * from file('test_02167.$format', '$format')"
done

$CLICKHOUSE_CLIENT -q "insert into table function file('test_02167.bin', 'auto', 'x uint64') select * from numbers(2)"
$CLICKHOUSE_CLIENT -q "select * from file('test_02167.bin', 'auto', 'x uint64')"
$CLICKHOUSE_CLIENT -q "select * from file('test_02167.bin', 'RowBinary', 'x uint64')"

$CLICKHOUSE_CLIENT -q "insert into table function file('test_02167.ndjson', 'auto', 'x uint64') select * from numbers(2)"
$CLICKHOUSE_CLIENT -q "select * from file('test_02167.ndjson')"
$CLICKHOUSE_CLIENT -q "select * from file('test_02167.ndjson', 'JSONEachRow', 'x uint64')"

$CLICKHOUSE_CLIENT -q "insert into table function file('test_02167.messagepack', 'auto', 'x uint64') select * from numbers(2)"
$CLICKHOUSE_CLIENT -q "select * from file('test_02167.messagepack') settings input_format_msgpack_number_of_columns=1"
$CLICKHOUSE_CLIENT -q "select * from file('test_02167.messagepack', 'MsgPack', 'x uint64')"

