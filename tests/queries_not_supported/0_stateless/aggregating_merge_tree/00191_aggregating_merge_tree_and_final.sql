SET query_mode='table';
DROP STREAM IF EXISTS aggregating_00191;
create stream aggregating_00191 (d date DEFAULT '2000-01-01', k uint64, u aggregate_function(uniq, uint64)) ENGINE = AggregatingMergeTree(d, k, 8192);

INSERT INTO aggregating_00191 (k, u) SELECT int_div(number, 100) AS k, uniqState(to_uint64(number % 100)) AS u FROM (SELECT * FROM system.numbers LIMIT 1000) GROUP BY k;
INSERT INTO aggregating_00191 (k, u) SELECT int_div(number, 100) AS k, uniqState(to_uint64(number % 100) + 50) AS u FROM (SELECT * FROM system.numbers LIMIT 500, 1000) GROUP BY k;
SELECT sleep(3);
SELECT k, finalize_aggregation(u) FROM aggregating_00191 FINAL order by k;

OPTIMIZE STREAM aggregating_00191;

SELECT k, finalize_aggregation(u) FROM aggregating_00191;
SELECT k, finalize_aggregation(u) FROM aggregating_00191 FINAL order by k;

DROP STREAM aggregating_00191;
