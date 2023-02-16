#!/usr/bin/env bash
# Tags: no-fasttest, long, no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --multiquery --multiline --query="""

DROP STREAM IF EXISTS t_01411;
DROP STREAM IF EXISTS t_01411_num;
drop stream if exists lc_dict_reading;

CREATE STREAM t_01411(
    str low_cardinality(string),
    arr array(low_cardinality(string)) default [str]
) ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO t_01411 (str) SELECT concat('asdf', to_string(number % 10000)) FROM numbers(100000);

CREATE STREAM t_01411_num(
    num uint8,
    arr array(low_cardinality(int64)) default [num]
) ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO t_01411_num (num) SELECT number % 1000 FROM numbers(100000);

create stream lc_dict_reading (val uint64, str stringWithDictionary, pat string) engine = MergeTree order by val;
insert into lc_dict_reading select number, if(number < 8192 * 4, number % 100, number) as s, s from system.numbers limit 100000;
"""

function go()
{

${CLICKHOUSE_CLIENT} --multiquery --multiline --query="""

select sum(to_uint64(str)), sum(to_uint64(pat)) from lc_dict_reading where val < 8129 or val > 8192 * 4;

SELECT count() FROM t_01411 WHERE str = 'asdf337';
SELECT count() FROM t_01411 WHERE arr[1] = 'asdf337';
SELECT count() FROM t_01411 WHERE has(arr, 'asdf337');
SELECT count() FROM t_01411 WHERE indexOf(arr, 'asdf337') > 0;

SELECT count() FROM t_01411 WHERE arr[1] = str;
SELECT count() FROM t_01411 WHERE has(arr, str);
SELECT count() FROM t_01411 WHERE indexOf(arr, str) > 0;

SELECT count() FROM t_01411_num WHERE num = 42;
SELECT count() FROM t_01411_num WHERE arr[1] = 42;
SELECT count() FROM t_01411_num WHERE has(arr, 42);
SELECT count() FROM t_01411_num WHERE indexOf(arr, 42) > 0;

SELECT count() FROM t_01411_num WHERE arr[1] = num;
SELECT count() FROM t_01411_num WHERE has(arr, num);
SELECT count() FROM t_01411_num WHERE indexOf(arr, num) > 0;
SELECT count() FROM t_01411_num WHERE indexOf(arr, num % 337) > 0;

SELECT indexOf(['a', 'b', 'c'], to_low_cardinality('a'));
SELECT indexOf(['a', 'b', NULL], to_low_cardinality('a'));
"""
}

for _ in `seq 1 32`; do go | grep -q "Exception" && echo 'FAIL' || echo 'OK' ||: & done

wait

${CLICKHOUSE_CLIENT} --multiquery --multiline --query="""
DROP STREAM IF EXISTS t_01411;
DROP STREAM IF EXISTS t_01411_num;
"""
