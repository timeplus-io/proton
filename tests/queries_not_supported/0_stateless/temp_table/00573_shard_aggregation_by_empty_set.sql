-- Tags: shard

CREATE TEMPORARY STREAM t_00573 (x uint8);

SET empty_result_for_aggregation_by_empty_set = 0;
SELECT count(), uniq(x), avg(x), avg(toNullable(x)), group_array(x), groupUniqArray(x) FROM remote('127.0.0.{1..10}', system.one) WHERE (rand() AS x) < 0;
SELECT count(), uniq(x), avg(x), avg(toNullable(x)), group_array(x), groupUniqArray(x) FROM remote('127.0.0.{1..10}', system.one) WHERE (rand() AS x) < 0 GROUP BY x;

SET empty_result_for_aggregation_by_empty_set = 1;
SELECT count(), uniq(x), avg(x), avg(toNullable(x)), group_array(x), groupUniqArray(x) FROM remote('127.0.0.{1..10}', system.one) WHERE (rand() AS x) < 0;
SELECT count(), uniq(x), avg(x), avg(toNullable(x)), group_array(x), groupUniqArray(x) FROM remote('127.0.0.{1..10}', system.one) WHERE (rand() AS x) < 0 GROUP BY x;
