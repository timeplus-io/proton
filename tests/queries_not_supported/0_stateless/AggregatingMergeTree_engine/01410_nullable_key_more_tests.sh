#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

test_func()
{
    engine=$1

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "drop stream if exists table_with_nullable_keys"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "create stream table_with_nullable_keys (nullable_int Nullable(uint32), nullable_str Nullable(string), nullable_lc low_cardinality(Nullable(string)), nullable_ints array(Nullable(uint32)), nullable_misc tuple(Nullable(string), Nullable(uint32)), nullable_val Map(uint32, Nullable(string)), value uint8) engine $engine order by (nullable_int, nullable_str, nullable_lc, nullable_ints, nullable_misc, nullable_val) settings allow_nullable_key = 1, index_granularity = 1"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "insert into table_with_nullable_keys select * replace (cast(nullable_val as Map(uint32, Nullable(string))) as nullable_val) from generateRandom('nullable_int Nullable(uint32), nullable_str Nullable(string), nullable_lc Nullable(string), nullable_ints array(Nullable(uint32)), nullable_misc tuple(Nullable(string), Nullable(uint32)), nullable_val array(tuple(uint32, Nullable(string))), value uint8', 1, 30, 30) limit 1024"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "select * from table_with_nullable_keys where nullable_str = (select randomPrintableASCII(30)) or nullable_str in (select randomPrintableASCII(30) from numbers(3)) format Null"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "select * from table_with_nullable_keys where nullable_lc = (select randomPrintableASCII(30)) or nullable_lc in (select randomPrintableASCII(30) from numbers(3)) format Null"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "select * from table_with_nullable_keys where nullable_ints = [1, 2, null] or nullable_ints in (select * from generateRandom('nullable_ints array(Nullable(uint32))', 1, 30, 30) limit 3) format Null"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "select * from table_with_nullable_keys where nullable_misc = (select (randomPrintableASCII(30), rand())) or nullable_misc in (select array_join([(randomPrintableASCII(30), null), (null, rand())]))"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "select * from table_with_nullable_keys where nullable_val = (select map(rand(), randomPrintableASCII(10), rand(2), randomPrintableASCII(20), rand(3), null)) or nullable_val in (select cast(nullable_ints as Map(uint32, Nullable(string))) from generateRandom('nullable_ints array(tuple(uint32, Nullable(string)))', 1, 30, 30) limit 3) format Null"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "drop stream table_with_nullable_keys"
}

test_func MergeTree
test_func AggregatingMergeTree
test_func ReplacingMergeTree
