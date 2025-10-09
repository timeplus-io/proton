#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --allow_suspicious_variant_types=1"

function test1_insert()
{
    echo "test1 insert"
    $CH_CLIENT -mnq "insert into test select number, NULL from numbers(3);
insert into test select number + 3, number from numbers(3);
insert into test select number + 6, ('str_' || to_string(number))::variant(string) from numbers(3);
insert into test select number + 9, ('lc_str_' || to_string(number))::low_cardinality(string) from numbers(3);
insert into test select number + 12, tuple_cast(number, number + 1)::tuple(a uint32, b uint32) from numbers(3);
insert into test select number + 15, range(number + 1)::array(uint64) from numbers(3);"
}

function test1_select()
{
    echo "test1 select"
    $CH_CLIENT -mnq "select v from test order by id;
select v.string from test order by id;
select v.uint64 from test order by id;
select v.\`low_cardinality(string)\` from test order by id;
select v.\`tuple(a uint32, b uint32)\` from test order by id;
select v.\`tuple(a uint32, b uint32)\`.a from test order by id;
select v.\`tuple(a uint32, b uint32)\`.b from test order by id;
select v.\`array(uint64)\` from test order by id;
select v.\`array(uint64)\`.size0 from test order by id;"
    echo "-----------------------------------------------------------------------------------------------------------"
}

function test2_insert()
{
    echo "test2 insert"
    $CH_CLIENT -mnq "insert into test select number, NULL from numbers(3);
insert into test select number + 3, number % 2 ? NULL : number from numbers(3);
insert into test select number + 6, number % 2 ? NULL : ('str_' || to_string(number))::variant(string) from numbers(3);
insert into test select number + 9, number % 2 ? CAST(NULL, 'variant(string, uint64, low_cardinality(string), tuple(a uint32, b uint32), array(uint64))') : CAST(('lc_str_' || to_string(number))::low_cardinality(string), 'variant(string, uint64, low_cardinality(string), tuple(a uint32, b uint32), array(uint64))') from numbers(3);
insert into test select number + 12, number % 2 ? CAST(NULL, 'variant(string, uint64, low_cardinality(string), tuple(a uint32, b uint32), array(uint64))') : CAST(tuple_cast(number, number + 1)::tuple(a uint32, b uint32), 'variant(string, uint64, low_cardinality(string), tuple(a uint32, b uint32), array(uint64))') from numbers(3);
insert into test select number + 15, number % 2 ? CAST(NULL, 'variant(string, uint64, low_cardinality(string), tuple(a uint32, b uint32), array(uint64))') : CAST(range(number + 1)::array(uint64), 'variant(string, uint64, low_cardinality(string), tuple(a uint32, b uint32), array(uint64))') from numbers(3);"
}

function test2_select()
{
    echo "test2 select"
    $CH_CLIENT -mnq "select v from test order by id;
select v.string from test order by id;
select v.uint64 from test order by id;
select v.\`low_cardinality(string)\` from test order by id;
select v.\`tuple(a uint32, b uint32)\` from test order by id;
select v.\`tuple(a uint32, b uint32)\`.a from test order by id;
select v.\`tuple(a uint32, b uint32)\`.b from test order by id;
select v.\`array(uint64)\` from test order by id;
select v.\`array(uint64)\`.size0 from test order by id;"
    echo "-----------------------------------------------------------------------------------------------------------"
}

function test3_insert()
{
    echo "test3 insert"
    $CH_CLIENT -q "insert into test with 'variant(string, uint64, low_cardinality(string), tuple(a uint32, b uint32), array(uint64))' as type select number, multi_if(number % 6 == 0, CAST(NULL, type), number % 6 == 1, CAST(('str_' || to_string(number))::variant(string), type), number % 6 == 2, CAST(number, type), number % 6 == 3, CAST(('lc_str_' || to_string(number))::low_cardinality(string), type), number % 6 == 4, CAST(tuple_cast(number, number + 1)::tuple(a uint32, b uint32), type), CAST(range(number + 1)::array(uint64), type)) as res from numbers(18);"
}

function test3_select()
{
    echo "test3 select"
    $CH_CLIENT -mnq "select v from test order by id;
select v.string from test order by id;
select v.uint64 from test order by id;
select v.\`low_cardinality(string)\` from test order by id;
select v.\`tuple(a uint32, b uint32)\` from test order by id;
select v.\`tuple(a uint32, b uint32)\`.a from test order by id;
select v.\`tuple(a uint32, b uint32)\`.b from test order by id;
select v.\`array(uint64)\` from test order by id;
select v.\`array(uint64)\`.size0 from test order by id;"
    echo "-----------------------------------------------------------------------------------------------------------"
}

function run()
{
    test1_insert
    test1_select
    if [ $1 == 1 ]; then
        $CH_CLIENT -q "optimize stream test final;"
        test1_select
    fi
    $CH_CLIENT -q "truncate stream test;"
    test2_insert
    test2_select
    if [ $1 == 1 ]; then
        $CH_CLIENT -q "optimize stream test final;"
        test2_select
    fi
    $CH_CLIENT -q "truncate stream test;"
    test3_insert
    test3_select
    if [ $1 == 1 ]; then
        $CH_CLIENT -q "optimize stream test final;"
        test3_select
    fi
    $CH_CLIENT -q "truncate stream test;"
}

$CH_CLIENT -q "drop stream if exists test;"

echo "Memory"
$CH_CLIENT -q "create stream test (id uint64, v variant(string, uint64, low_cardinality(string), tuple(a uint32, b uint32), array(uint64))) engine=Memory;"
run 0
$CH_CLIENT -q "drop stream test;"

echo "MergeTree compact"
$CH_CLIENT -q "create stream test (id uint64, v variant(string, uint64, low_cardinality(string), tuple(a uint32, b uint32), array(uint64))) engine=MergeTree order by id settings min_rows_for_wide_part=100000000, min_bytes_for_wide_part=1000000000, index_granularity_bytes=10485760, index_granularity=8192;"
run 1
$CH_CLIENT -q "drop stream test;"

echo "MergeTree wide"
$CH_CLIENT -q "create stream test (id uint64, v variant(string, uint64, low_cardinality(string), tuple(a uint32, b uint32), array(uint64))) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, index_granularity_bytes=10485760, index_granularity=8192;"
run 1
$CH_CLIENT -q "drop stream test;"
