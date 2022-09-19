-- Tags: no-parallel

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
DROP STREAM IF EXISTS t3;

create stream t1
(
    `n` int8
)

COMMENT 'this is a temtorary table';

create stream t2
(
    `n` int8
)
ENGINE = MergeTree
ORDER BY n
COMMENT 'this is a MergeTree table';

create stream t3
(
    `n` int8
)
 
COMMENT 'this is a Log table';

SELECT
    name,
    comment
FROM system.tables
WHERE name IN ('t1', 't2', 't3') AND database = currentDatabase() order by name;

SHOW create stream t1;

DROP STREAM t1;
DROP STREAM t2;
DROP STREAM t3;
