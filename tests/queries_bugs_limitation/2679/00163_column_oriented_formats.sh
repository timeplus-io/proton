#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


FORMATS=('Parquet' 'Arrow' 'ORC')

for format in "${FORMATS[@]}"
do
    echo $format
    $CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS 00163_column_oriented SYNC"
    $CLICKHOUSE_CLIENT -q "CREATE STREAM 00163_column_oriented(ClientEventTime DateTime('Europe/Moscow'), MobilePhoneModel string, ClientIP6 fixed_string(16)) ENGINE=File($format)"
    $CLICKHOUSE_CLIENT -q "INSERT INTO 00163_column_oriented SELECT ClientEventTime, MobilePhoneModel, ClientIP6 FROM test.hits ORDER BY ClientEventTime, MobilePhoneModel, ClientIP6 LIMIT 100"
    $CLICKHOUSE_CLIENT -q "SELECT ClientEventTime from 00163_column_oriented" | md5sum
    $CLICKHOUSE_CLIENT -q "SELECT MobilePhoneModel from 00163_column_oriented" | md5sum
    $CLICKHOUSE_CLIENT -q "SELECT ClientIP6 from 00163_column_oriented" | md5sum
    $CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS 00163_column_oriented SYNC"
done
