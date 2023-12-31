SET send_logs_level = 'fatal';

DROP STREAM IF EXISTS test_00808;
create stream test_00808(date date, id int8, name string, value int64, sign int8) ENGINE = CollapsingMergeTree(sign) ORDER BY (id, date);

INSERT INTO test_00808 VALUES('2000-01-01', 1, 'test string 1', 1, 1);
INSERT INTO test_00808 VALUES('2000-01-01', 2, 'test string 2', 2, 1);

SET enable_optimize_predicate_expression = 1;

SELECT '-------ENABLE OPTIMIZE PREDICATE-------';
SELECT * FROM (SELECT * FROM test_00808 FINAL) WHERE id = 1;
SELECT * FROM (SELECT * FROM test_00808 ORDER BY id LIMIT 1) WHERE id = 1;
SELECT * FROM (SELECT id FROM test_00808 GROUP BY id LIMIT 1 BY id) WHERE id = 1;

SET force_primary_key = 1;

SELECT '-------FORCE PRIMARY KEY-------';
SELECT * FROM (SELECT * FROM test_00808 LIMIT 1) WHERE id = 1; -- { serverError 277 }
SELECT * FROM (SELECT id FROM test_00808 GROUP BY id LIMIT 1 BY id) WHERE id = 1; -- { serverError 277 }

SELECT '-------CHECK STATEFUL FUNCTIONS-------';
SELECT n, z, changed FROM (
  SELECT n, z, runningDifferenceStartingWithFirstValue(n) AS changed FROM (
     SELECT ts, n,z FROM system.one ARRAY JOIN [1,3,4,5,6] AS ts,
        [1,2,2,2,1] AS n, ['a', 'a', 'b', 'a', 'b'] AS z
      ORDER BY n, ts DESC
  )
) WHERE changed = 0;


SELECT array_join(array_map(x -> x, arraySort(group_array((ts, n))))) AS k FROM (
  SELECT ts, n, z FROM system.one ARRAY JOIN [1, 3, 4, 5, 6] AS ts, [1, 2, 2, 2, 1] AS n, ['a', 'a', 'b', 'a', 'b'] AS z
  ORDER BY n ASC, ts DESC
) WHERE z = 'a' GROUP BY z;


DROP STREAM IF EXISTS test_00808;

SELECT '-------finalizeAggregation should not be stateful (issue #14847)-------';

DROP STREAM IF EXISTS test_00808_push_down_with_finalizeAggregation;

create stream test_00808_push_down_with_finalizeAggregation ENGINE = AggregatingMergeTree
ORDER BY n AS
SELECT
    int_div(number, 25) AS n,
    avgState(number) AS s
FROM numbers(2500)
GROUP BY n;

SET force_primary_key = 1, enable_optimize_predicate_expression = 1;

SELECT *
FROM
(
    SELECT
        n,
        finalize_aggregation(s)
    FROM test_00808_push_down_with_finalizeAggregation
)
WHERE (n >= 2) AND (n <= 5);

EXPLAIN SYNTAX SELECT *
FROM
(
    SELECT
        n,
        finalize_aggregation(s)
    FROM test_00808_push_down_with_finalizeAggregation
)
WHERE (n >= 2) AND (n <= 5);

DROP STREAM IF EXISTS test_00808_push_down_with_finalizeAggregation;
