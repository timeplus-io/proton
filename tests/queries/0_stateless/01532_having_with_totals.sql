SET query_mode = 'table';
drop stream if exists local_t;
create stream local_t engine Log as select 1 a;

SELECT '127.0.0.{1,2}';
SELECT *
FROM
(
    SELECT a
    FROM remote('127.0.0.{1,2}', currentDatabase(), local_t)
    GROUP BY a
        WITH TOTALS
)
WHERE a IN 
(
    SELECT 1
);

SELECT '127.0.0.1';
SELECT *
FROM
(
    SELECT a
    FROM remote('127.0.0.1', currentDatabase(), local_t)
    GROUP BY a
        WITH TOTALS
)
WHERE a IN 
(
    SELECT 1
);

SELECT 'with explicit having';
SELECT
    a,
    count()
FROM remote('127.0.0.{1,2}', currentDatabase(), local_t)
GROUP BY a
    WITH TOTALS
HAVING a IN 
(
    SELECT 1
);


drop stream if exists local_t;