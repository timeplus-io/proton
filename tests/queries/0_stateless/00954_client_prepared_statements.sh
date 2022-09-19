#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS ps";
$CLICKHOUSE_CLIENT -q "create stream ps (i uint8, s string, d DateTime) ";

$CLICKHOUSE_CLIENT -q "INSERT INTO ps VALUES (1, 'Hello, world', '2005-05-05 05:05:05')";
$CLICKHOUSE_CLIENT -q "INSERT INTO ps VALUES (2, 'test', '2005-05-25 15:00:00')";

$CLICKHOUSE_CLIENT --max_threads=1 --param_id=1 \
    -q "SELECT * FROM ps WHERE i = {id:uint8}";
$CLICKHOUSE_CLIENT --max_threads=1 --param_phrase='Hello, world' \
    -q "SELECT * FROM ps WHERE s = {phrase:string}";
$CLICKHOUSE_CLIENT --max_threads=1 --param_date='2005-05-25 15:00:00' \
    -q "SELECT * FROM ps WHERE d = {date:DateTime}";
$CLICKHOUSE_CLIENT --max_threads=1 --param_id=2 --param_phrase='test' \
    -q "SELECT * FROM ps WHERE i = {id:uint8} and s = {phrase:string}";

$CLICKHOUSE_CLIENT -q "SELECT {s:string}" 2>&1 | grep -oP '^Code: 456\.'

$CLICKHOUSE_CLIENT -q "DROP STREAM ps";


$CLICKHOUSE_CLIENT --param_test abc --query 'SELECT {test:string}'
$CLICKHOUSE_CLIENT --param_test=abc --query 'SELECT {test:string}'

$CLICKHOUSE_CLIENT --param_test 'Hello, world' --query 'SELECT {test:string}'
$CLICKHOUSE_CLIENT --param_test='Hello, world' --query 'SELECT {test:string}'

$CLICKHOUSE_CLIENT --param_test '' --query 'SELECT length({test:string})'
$CLICKHOUSE_CLIENT --param_test='' --query 'SELECT length({test:string})'
