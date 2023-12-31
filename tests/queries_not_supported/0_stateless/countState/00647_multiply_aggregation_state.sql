 

SELECT countMerge(x) AS y FROM ( SELECT countState() * 2 AS x FROM ( SELECT 1 ));
SELECT countMerge(x) AS y FROM ( SELECT countState() * 0 AS x FROM ( SELECT 1 UNION ALL SELECT 2));
SELECT sumMerge(y) AS z FROM ( SELECT sumState(x) * 11 AS y FROM ( SELECT 1 AS x UNION ALL SELECT 2 AS x));
SELECT countMerge(x) AS y FROM ( SELECT 2 * countState() AS x FROM ( SELECT 1 ));
SELECT countMerge(x) AS y FROM ( SELECT 0 * countState() AS x FROM ( SELECT 1 UNION ALL SELECT 2));
SELECT sumMerge(y) AS z FROM ( SELECT 3 * sumState(x) * 2 AS y FROM ( SELECT 1 AS x UNION ALL SELECT 2 AS x));

DROP STREAM IF EXISTS mult_aggregation;
create stream mult_aggregation(a uint32, b uint32) ;
INSERT INTO mult_aggregation VALUES(1, 1);
INSERT INTO mult_aggregation VALUES(1, 3);

SELECT sumMerge(x * 5), sumMerge(x) FROM (SELECT sumState(b) AS x FROM mult_aggregation);
SELECT uniqMerge(x * 10) FROM (SELECT uniq_state(b) AS x FROM mult_aggregation);
SELECT maxMerge(x * 10) FROM (SELECT maxState(b) AS x FROM mult_aggregation);
SELECT avgMerge(x * 10) FROM (SELECT avgState(b) AS x FROM mult_aggregation);

SELECT groupArrayMerge(y * 5) FROM (SELECT groupArrayState(x) AS y FROM (SELECT 1 AS x));
SELECT groupArrayMerge(2)(y * 5) FROM (SELECT groupArrayState(2)(x) AS y FROM (SELECT 1 AS x));
SELECT groupUniqArrayMerge(y * 5) FROM (SELECT groupUniqArrayState(x) AS y FROM (SELECT 1 AS x));

SELECT sumMerge(y * a) FROM (SELECT a, sumState(b) AS y FROM mult_aggregation GROUP BY a); -- { serverError 44}

DROP STREAM IF EXISTS mult_aggregation;
