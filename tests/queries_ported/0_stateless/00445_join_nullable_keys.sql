SET join_use_nulls = 0;
SET any_join_distinct_right_table_keys = 1;

SELECT k, a, b
FROM
(
    SELECT null_if(number, 7) AS k, to_string(number) AS a FROM system.numbers LIMIT 10
) as js1
ANY INNER JOIN
(
    SELECT number AS k, to_string(number) AS b FROM system.numbers LIMIT 5, 10
) as js2 USING (k) ORDER BY k;

SELECT k, a, b
FROM
(
    SELECT number AS k, to_string(number) AS a FROM system.numbers LIMIT 10
) as js1
ANY LEFT JOIN
(
    SELECT null_if(number, 8) AS k, to_string(number) AS b FROM system.numbers LIMIT 5, 10
) as js2 USING (k) ORDER BY k;

SELECT k, a, b
FROM
(
    SELECT null_if(number, 7) AS k, to_string(number) AS a FROM system.numbers LIMIT 10
) as js1
ANY RIGHT JOIN
(
    SELECT null_if(number, 8) AS k, to_string(number) AS b FROM system.numbers LIMIT 5, 10
) as js2 USING (k) ORDER BY k;

SELECT k, b
FROM
(
    SELECT number + 1 AS k FROM numbers(10)
) as js1
RIGHT JOIN
(
    SELECT null_if(number, if(number % 2 == 0, number, 0)) AS k, number AS b FROM numbers(10)
) as js2 USING (k) ORDER BY k, b;
