DROP STREAM IF EXISTS test;

CREATE STREAM test
(
    `t` uint8,
    `flag` uint8,
    `id` uint8
)
ENGINE = MergeTree
PARTITION BY t
ORDER BY (t, id)
SETTINGS index_granularity = 8192;

INSERT INTO test VALUES (1,0,1),(1,0,2),(1,0,3),(1,0,4),(1,0,5),(1,0,6),(1,1,7),(0,0,7);

set query_plan_filter_push_down = true;

SELECT id, flag FROM test t1
INNER JOIN  (SELECT DISTINCT id FROM test) AS t2 ON t1.id = t2.id
WHERE flag = 0 and t = 1 AND id NOT IN (SELECT 1 WHERE 0)
ORDER BY id;

DROP STREAM IF EXISTS test;
