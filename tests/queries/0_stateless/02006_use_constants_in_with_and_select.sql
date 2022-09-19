SELECT
    1 AS max_size,
    group_array(max_size)(col)
FROM
    (
        SELECT 1 AS col
        UNION ALL
        SELECT 2
    );

WITH 1 AS max_size
SELECT group_array(max_size)(col)
FROM
     (
         SELECT 1 as col
         UNION ALL
         SELECT 2
     );

WITH 0.1 AS level
SELECT quantile(level)(number)
FROM numbers(1000);

SELECT 0.1 AS level, quantile(level)(number)
FROM numbers(1000);

WITH
    0.1 AS level,
    1 AS max_size
SELECT group_array(max_size)(col)
FROM
    (
        SELECT quantile(level)(number) AS col
        FROM numbers(1000)
    );

