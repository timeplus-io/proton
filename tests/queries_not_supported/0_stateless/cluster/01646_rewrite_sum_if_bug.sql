DROP STREAM IF EXISTS t;
create stream t( s string ) Engine=Memory as select array_join (['a','b','c']);

SELECT round((sum(multiIf(s IN ('a', 'b'), 1, 0)) / count()) * 100) AS r
FROM cluster('test_cluster_two_shards', current_database(), t);

DROP STREAM t;


DROP STREAM IF EXISTS test_alias;

CREATE STREAM test_alias(`a` int64, `b` int64, `c` int64, `day` Date, `rtime` DateTime) ENGINE = Memory
as select 0, 0, 0, '2022-01-01', 0 from zeros(10);

WITH 
    sum(if((a >= 0) AND (b != 100) AND (c = 0), 1, 0)) AS r1, 
    sum(if((a >= 0) AND (b != 100) AND (c > 220), 1, 0)) AS r2 
SELECT 
    (int_div(to_uint32(rtime), 20) * 20) * 1000 AS t, 
    (r1 * 100) / (r1 + r2) AS m
FROM cluster('test_cluster_two_shards', current_database(), test_alias)
WHERE day = '2022-01-01'
GROUP BY t
ORDER BY t ASC;

DROP STREAM test_alias;
