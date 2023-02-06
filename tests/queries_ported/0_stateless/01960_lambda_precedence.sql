SELECT
    1000 AS a,
    array_map(a -> (a + 1), [1, 2, 3]),
    a + 10 as c;


-- https://github.com/ClickHouse/ClickHouse/issues/5046
SELECT sum(c1) AS v
FROM
    (
     SELECT
         1 AS c1,
         ['v'] AS c2
        )
WHERE array_exists(v -> (v = 'v'), c2);


SELECT sum(c1) AS v
FROM
    (
     SELECT
         1 AS c1,
         ['v'] AS c2,
         ['d'] AS d
        )
WHERE array_exists(i -> (d = ['d']), c2);
