#!/usr/bin/env bash
# Tags: race, no-parallel

# This is a monkey test used to trigger sanitizers.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP DATABASE IF EXISTS dictdb_01076;"
$CLICKHOUSE_CLIENT --query="CREATE DATABASE dictdb_01076;"

$CLICKHOUSE_CLIENT --query="
CREATE STREAM dictdb_01076.table_datarace
(
  key_column UUID,
  value float64
)
ENGINE = MergeTree()
ORDER BY key_column;
"

$CLICKHOUSE_CLIENT --query="
INSERT INTO dictdb_01076.table_datarace VALUES ('cd5db34f-0c25-4375-b10e-bfb3708ddc72', 1.1), ('cd5db34f-0c25-4375-b10e-bfb3708ddc72', 2.2), ('cd5db34f-0c25-4375-b10e-bfb3708ddc72', 3.3);
"

$CLICKHOUSE_CLIENT --query="
CREATE DICTIONARY IF NOT EXISTS dictdb_01076.dict_datarace
(
  key_column uint64,
  value float64 DEFAULT 77.77
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' STREAM 'table_datarace' DB 'dictdb_01076'))
LIFETIME(1)
LAYOUT(CACHE(SIZE_IN_CELLS 10));
"

function thread1()
{
    for _ in {1..50}
    do
        # This query will be ended with exception, because source dictionary has UUID as a key type.
        $CLICKHOUSE_CLIENT --query="SELECT dictGetFloat64('dictdb_01076.dict_datarace', 'value', to_uint64(1));"
    done
}


function thread2()
{
    for _ in {1..50}
    do
        # This query will be ended with exception, because source dictionary has UUID as a key type.
        $CLICKHOUSE_CLIENT --query="SELECT dictGetFloat64('dictdb_01076.dict_datarace', 'value', to_uint64(2));"
    done
}

export -f thread1;
export -f thread2;

TIMEOUT=5

timeout $TIMEOUT bash -c thread1 > /dev/null 2>&1 &
timeout $TIMEOUT bash -c thread2 > /dev/null 2>&1 &

wait

echo OK

$CLICKHOUSE_CLIENT --query="DROP DICTIONARY dictdb_01076.dict_datarace;"
$CLICKHOUSE_CLIENT --query="DROP STREAM dictdb_01076.table_datarace;"
$CLICKHOUSE_CLIENT --query="DROP DATABASE dictdb_01076;"
