#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS db_01038"

$CLICKHOUSE_CLIENT --query "CREATE DATABASE db_01038"


$CLICKHOUSE_CLIENT --query "
CREATE STREAM db_01038.table_for_dict
(
  key_column uint64,
  value float64
)
ENGINE = MergeTree()
ORDER BY key_column"

$CLICKHOUSE_CLIENT --query "INSERT INTO db_01038.table_for_dict VALUES (1, 1.1)"

$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY db_01038.dict_with_zero_min_lifetime
(
    key_column uint64,
    value float64 DEFAULT 77.77
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' STREAM 'table_for_dict' DB 'db_01038'))
LIFETIME(1)
LAYOUT(FLAT())"

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('db_01038.dict_with_zero_min_lifetime', 'value', to_uint64(1))"

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('db_01038.dict_with_zero_min_lifetime', 'value', to_uint64(2))"

$CLICKHOUSE_CLIENT --query "INSERT INTO db_01038.table_for_dict VALUES (2, 2.2)"


function check()
{

    query_result=$($CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('db_01038.dict_with_zero_min_lifetime', 'value', to_uint64(2))")

    while [ "$query_result" != "2.2" ]
    do
        query_result=$($CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('db_01038.dict_with_zero_min_lifetime', 'value', to_uint64(2))")
    done
}


export -f check;

timeout 10 bash -c check

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('db_01038.dict_with_zero_min_lifetime', 'value', to_uint64(1))"

$CLICKHOUSE_CLIENT --query "SELECT dictGetFloat64('db_01038.dict_with_zero_min_lifetime', 'value', to_uint64(2))"

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS db_01038"
