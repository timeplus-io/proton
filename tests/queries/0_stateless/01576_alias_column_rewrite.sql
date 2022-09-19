SET query_mode = 'table';
DROP STREAM IF EXISTS test_table;
create stream test_table
(
 `timestamp` DateTime,
 `value` uint64,
 `day` date ALIAS to_date(timestamp),
 `day1` date ALIAS day + 1,
 `day2` date ALIAS day1 + 1,
 `time` DateTime ALIAS timestamp
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY timestamp SETTINGS index_granularity = 1;


INSERT INTO test_table(timestamp, value) SELECT to_datetime('2020-01-01 12:00:00'), 1 FROM numbers(10);
INSERT INTO test_table(timestamp, value) SELECT to_datetime('2020-01-02 12:00:00'), 1 FROM numbers(10);
INSERT INTO test_table(timestamp, value) SELECT to_datetime('2020-01-03 12:00:00'), 1 FROM numbers(10);

set optimize_respect_aliases = 1;
SELECT 'test-partition-prune';

SELECT COUNT() = 10 FROM test_table WHERE day = '2020-01-01' SETTINGS max_rows_to_read = 10;
SELECT t = '2020-01-03' FROM (SELECT day AS t FROM test_table WHERE t = '2020-01-03' GROUP BY t SETTINGS max_rows_to_read = 10);
SELECT COUNT() = 10 FROM test_table WHERE day = '2020-01-01' UNION ALL SELECT 1 FROM numbers(1) SETTINGS max_rows_to_read = 11;
SELECT  COUNT() = 0 FROM (SELECT  to_date('2019-01-01') AS  day, day AS t   FROM test_table PREWHERE t = '2020-01-03'  WHERE t  = '2020-01-03' GROUP BY t );



SELECT 'test-join';
SELECT day = '2020-01-03'
FROM
(
 SELECT to_date('2020-01-03') AS day
 FROM numbers(1)
) AS a
INNER JOIN
(
 SELECT day
 FROM test_table
 WHERE day = '2020-01-03'
 GROUP BY day
) AS b ON a.day = b.day SETTINGS max_rows_to_read = 11;

SELECT day = '2020-01-01'
FROM
(
 SELECT day
 FROM test_table
 WHERE day = '2020-01-01'
 GROUP BY day
) AS a
INNER JOIN
(
 SELECT to_date('2020-01-01') AS day
 FROM numbers(1)
) AS b ON a.day = b.day SETTINGS max_rows_to_read = 11;


SELECT 'alias2alias';
SELECT COUNT() = 10 FROM test_table WHERE day1 = '2020-01-02' SETTINGS max_rows_to_read = 10;
SELECT t = '2020-01-03' FROM (SELECT day1 AS t FROM test_table WHERE t = '2020-01-03' GROUP BY t SETTINGS max_rows_to_read = 10);
SELECT t = '2020-01-03' FROM (SELECT day2 AS t FROM test_table WHERE t = '2020-01-03' GROUP BY t SETTINGS max_rows_to_read = 10);
SELECT COUNT() = 10 FROM test_table WHERE day1 = '2020-01-03' UNION ALL SELECT 1 FROM numbers(1) SETTINGS max_rows_to_read = 11;
SELECT  COUNT() = 0 FROM (SELECT  to_date('2019-01-01') AS  day1, day1 AS t   FROM test_table PREWHERE t = '2020-01-03'  WHERE t  = '2020-01-03' GROUP BY t );
SELECT day1 = '2020-01-04' FROM test_table PREWHERE day1 = '2020-01-04'  WHERE day1 = '2020-01-04' GROUP BY day1 SETTINGS max_rows_to_read = 10;


ALTER STREAM test_table add column array array(uint8) default [1, 2, 3];
ALTER STREAM test_table add column struct.key array(uint8) default [2, 4, 6], add column struct.value array(uint8) alias array;


SELECT 'array-join';
set max_rows_to_read = 10;
SELECT count() == 10 FROM test_table WHERE day = '2020-01-01';
SELECT sum(struct.key) == 30, sum(struct.value) == 30 FROM (SELECT struct.key, struct.value FROM test_table array join struct WHERE day = '2020-01-01');


SELECT 'lambda';
-- lambda parameters in filter should not be rewrite
SELECT count() == 10 FROM test_table WHERE  array_map((day) -> day + 1, [1,2,3]) [1] = 2 AND day = '2020-01-03';

set max_rows_to_read = 0;

SELECT 'optimize_read_in_order';
EXPLAIN SELECT day AS s FROM test_table ORDER BY s LIMIT 1 SETTINGS optimize_read_in_order = 0;
EXPLAIN SELECT day AS s FROM test_table ORDER BY s LIMIT 1 SETTINGS optimize_read_in_order = 1;
EXPLAIN SELECT to_date(timestamp) AS s FROM test_table ORDER BY to_date(timestamp) LIMIT 1 SETTINGS optimize_read_in_order = 1;


SELECT 'optimize_aggregation_in_order';
EXPLAIN SELECT day, count() AS s FROM test_table GROUP BY day SETTINGS optimize_aggregation_in_order = 0;
EXPLAIN SELECT day, count() AS s FROM test_table GROUP BY day SETTINGS optimize_aggregation_in_order = 1;
EXPLAIN SELECT to_date(timestamp), count() AS s FROM test_table GROUP BY to_date(timestamp) SETTINGS optimize_aggregation_in_order = 1;

DROP STREAM test_table;


SELECT 'second-index';
DROP STREAM IF EXISTS test_index;
create stream test_index
(
    `key_string` string,
    `key_uint32` ALIAS to_uint32(key_string),
    INDEX idx to_uint32(key_string) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY tuple()
PRIMARY KEY tuple()
ORDER BY key_string SETTINGS index_granularity = 1;

INSERT INTO test_index SELECT * FROM numbers(10);
set max_rows_to_read = 1;
SELECT COUNT() == 1 FROM test_index WHERE key_uint32 = 1;
SELECT COUNT() == 1 FROM test_index WHERE to_uint32(key_string) = 1;
DROP STREAM IF EXISTS test_index;


-- check alias column can be used to match projections
drop stream if exists p;
create stream pd (dt DateTime, i int, dt_m DateTime alias to_start_of_minute(dt)) engine Distributed(test_shard_localhost, currentDatabase(), 'pl');
create stream pl (dt DateTime, i int, projection p (select sum(i) group by to_start_of_minute(dt))) engine MergeTree order by dt;

insert into pl values ('2020-10-24', 1);

select sum(i) from pd group by dt_m settings allow_experimental_projection_optimization = 1, force_optimize_projection = 1;

drop stream pd;
drop stream pl;

drop stream if exists t;

create temporary table t (x uint64, y alias x);
insert into t values (1);
select sum(x), sum(y) from t;

drop stream t;
