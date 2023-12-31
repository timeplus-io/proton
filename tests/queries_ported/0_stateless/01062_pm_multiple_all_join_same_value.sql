SET max_memory_usage = 50000000;
SET join_algorithm = 'partial_merge';

SELECT count(1) FROM (
    SELECT t2.n FROM numbers(10) as t1
    JOIN (SELECT to_uint32(1) AS k, number as n FROM numbers(100)) as t2 ON to_uint32(t1.number) = t2.k
    JOIN (SELECT to_uint32(1) AS k, number as n FROM numbers(100)) as t3 ON t2.k = t3.k
    JOIN (SELECT to_uint32(1) AS k, number as n FROM numbers(100)) as t4 ON t2.k = t4.k
);
