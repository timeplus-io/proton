DROP STREAM IF EXISTS test02416;

CREATE STREAM test02416(a uint64, b uint64) ENGINE=MergeTree() ORDER BY (a, b);

INSERT INTO test02416 SELECT number % 2 as a, number as b FROM numbers(10);

-- { echoOn }
SELECT count() AS amount, a, b, GROUPING(a, b) FROM test02416 GROUP BY GROUPING SETS ((a, b), (a), ()) ORDER BY (amount, a, b);

SELECT count() AS amount, a, b, GROUPING(a, b) FROM test02416 GROUP BY ROLLUP(a, b) ORDER BY (amount, a, b);

-- { echoOff }
DROP STREAM test02416;

