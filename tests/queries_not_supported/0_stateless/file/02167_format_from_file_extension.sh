#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function cleanup()
{
    # this command expects an error message like 'Code: 107. DB::Exception: Received <...> nonexist.txt doesn't exist. (FILE_DOESNT_EXIST)'
    user_files_path=$($CLICKHOUSE_CLIENT --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep -E '^Code: 107.*FILE_DOESNT_EXIST' | head -1 | awk '{gsub("/nonexist.txt","",$9); print $9}')
    rm $user_files_path/test_02167.*
}
trap cleanup EXIT

for format in TSV TabSeparated TSVWithNames TSVWithNamesAndTypes CSV Parquet ORC Arrow JSONEachRow JSONCompactEachRow CustomSeparatedWithNamesAndTypes
do
    $CLICKHOUSE_CLIENT -q "insert into stream function file('test_02167.$format', 'auto', 'x uint64') select * from numbers(2)"
    $CLICKHOUSE_CLIENT -q "select * from file('test_02167.$format')"
    $CLICKHOUSE_CLIENT -q "select * from file('test_02167.$format', '$format')"
done

$CLICKHOUSE_CLIENT -q "insert into stream function file('test_02167.bin', 'auto', 'x uint64') select * from numbers(2)"
$CLICKHOUSE_CLIENT -q "select * from file('test_02167.bin', 'auto', 'x uint64')"
$CLICKHOUSE_CLIENT -q "select * from file('test_02167.bin', 'RowBinary', 'x uint64')"

$CLICKHOUSE_CLIENT -q "insert into stream function file('test_02167.ndjson', 'auto', 'x uint64') select * from numbers(2)"
$CLICKHOUSE_CLIENT -q "select * from file('test_02167.ndjson')"
$CLICKHOUSE_CLIENT -q "select * from file('test_02167.ndjson', 'JSONEachRow', 'x uint64')"

$CLICKHOUSE_CLIENT -q "insert into stream function file('test_02167.messagepack', 'auto', 'x uint64') select * from numbers(2)"
$CLICKHOUSE_CLIENT -q "select * from file('test_02167.messagepack') settings input_format_msgpack_number_of_columns=1"
$CLICKHOUSE_CLIENT -q "select * from file('test_02167.messagepack', 'MsgPack', 'x uint64')"

