DROP STREAM IF EXISTS t_01411;

create stream t_01411(
    str LowCardinality(string),
    arr array(LowCardinality(string)) default [str]
) ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO t_01411 (str) SELECT concat('asdf', to_string(number % 10000)) FROM numbers(1000000);

SELECT count() FROM t_01411 WHERE str = 'asdf337';
SELECT count() FROM t_01411 WHERE arr[1] = 'asdf337';
SELECT count() FROM t_01411 WHERE has(arr, 'asdf337');
SELECT count() FROM t_01411 WHERE indexOf(arr, 'asdf337') > 0;

SELECT count() FROM t_01411 WHERE arr[1] = str;
SELECT count() FROM t_01411 WHERE has(arr, str);
SELECT count() FROM t_01411 WHERE indexOf(arr, str) > 0;

DROP STREAM IF EXISTS t_01411;
DROP STREAM IF EXISTS t_01411_num;

create stream t_01411_num(
    num uint8,
    arr array(LowCardinality(int64)) default [num]
) ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO t_01411_num (num) SELECT number % 1000 FROM numbers(1000000);

SELECT count() FROM t_01411_num WHERE num = 42;
SELECT count() FROM t_01411_num WHERE arr[1] = 42;
SELECT count() FROM t_01411_num WHERE has(arr, 42);
SELECT count() FROM t_01411_num WHERE indexOf(arr, 42) > 0;

SELECT count() FROM t_01411_num WHERE arr[1] = num;
SELECT count() FROM t_01411_num WHERE has(arr, num);
SELECT count() FROM t_01411_num WHERE indexOf(arr, num) > 0;
SELECT count() FROM t_01411_num WHERE indexOf(arr, num % 337) > 0;

-- Checking Arr(string) and LC(string)
SELECT indexOf(['a', 'b', 'c'], toLowCardinality('a'));

-- Checking Arr(Nullable(string)) and LC(string)
SELECT indexOf(['a', 'b', NULL], toLowCardinality('a'));

DROP STREAM IF EXISTS t_01411_num;
