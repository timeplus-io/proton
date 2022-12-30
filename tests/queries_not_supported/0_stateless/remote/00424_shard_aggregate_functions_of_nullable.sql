-- Tags: shard

SELECT avg(array_join([NULL]));
SELECT avg(array_join([NULL, 1]));
SELECT avg(array_join([NULL, 1, 2]));

SELECT quantileExactWeighted(0.5)(x, y) FROM
(
    SELECT CAST(NULL AS Nullable(uint8)) AS x, CAST(1 AS Nullable(uint8)) AS y
    UNION ALL
    SELECT CAST(2 AS Nullable(uint8)) AS x, CAST(NULL AS Nullable(uint8)) AS y
);

SELECT quantileExactWeighted(0.5)(x, y) FROM
(
    SELECT CAST(1 AS Nullable(uint8)) AS x, CAST(0 AS Nullable(uint8)) AS y
    UNION ALL
    SELECT CAST(NULL AS Nullable(uint8)) AS x, CAST(1 AS Nullable(uint8)) AS y
    UNION ALL
    SELECT CAST(2 AS Nullable(uint8)) AS x, CAST(NULL AS Nullable(uint8)) AS y
    UNION ALL
    SELECT CAST(number AS Nullable(uint8)) AS x, CAST(number AS Nullable(uint8)) AS y FROM system.numbers LIMIT 10
);

SELECT quantileExactWeighted(0.5)(x, y) FROM
(
    SELECT CAST(NULL AS Nullable(uint8)) AS x, 1 AS y
    UNION ALL
    SELECT CAST(2 AS Nullable(uint8)) AS x, 1 AS y
);

SELECT quantileExactWeighted(0.5)(x, y) FROM
(
    SELECT CAST(NULL AS Nullable(uint8)) AS x, 1 AS y
);

SELECT
    sum(1 + CAST(dummy AS Nullable(uint8))) AS res1, to_type_name(res1) AS t1,
    sum(1 + null_if(dummy, 0)) AS res2, to_type_name(res2) AS t2
FROM remote('127.0.0.{2,3}', system.one);

SELECT CAST(NULL AS Nullable(uint64)) FROM system.numbers LIMIT 2
