-- Tags: global

DROP STREAM IF EXISTS test1;
DROP STREAM IF EXISTS test2;

create stream test1 (a uint8, b array(DateTime)) ENGINE Memory;
create stream test2 as test1 ENGINE Distributed(test_shard_localhost, currentDatabase(), test1);

INSERT INTO test1 VALUES (1, [1, 2, 3]);

SELECT 1
FROM test2 AS test2
ARRAY JOIN array_filter(t -> (t GLOBAL IN
    (
        SELECT DISTINCT now() AS `ym:a`
        WHERE 1
    )), test2.b) AS test2_b
WHERE 1;

DROP STREAM test1;
DROP STREAM test2;
