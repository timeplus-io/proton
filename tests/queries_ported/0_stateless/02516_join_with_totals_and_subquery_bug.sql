SELECT *
FROM
(
    SELECT 1 AS a
) AS t1
INNER JOIN
(
    SELECT 1 AS a
    GROUP BY 1
        WITH TOTALS
    UNION ALL
    SELECT 1
    GROUP BY 1
        WITH TOTALS
) AS t2 USING (a)
SETTINGS allow_experimental_analyzer=1;

-- SELECT a
-- FROM
-- (
--     SELECT
--         NULL AS a,
--         NULL AS b,
--         NULL AS c
--     UNION ALL
--     SELECT
--         100000000000000000000.,
--         NULL,
--         NULL
--     WHERE 0
--     GROUP BY
--         GROUPING SETS ((NULL))
--         WITH TOTALS
-- ) AS js1
-- ALL LEFT JOIN
-- (
--     SELECT
--         NULL AS a,
--         2147483647 AS d
--     GROUP BY
--         NULL,
--         '214748364.8'
--         WITH CUBE
--         WITH TOTALS
--     UNION ALL
--     SELECT
--         2147483646,
--         NULL
--     GROUP BY
--         base58_Encode(materialize(NULL)),
--         NULL
--         WITH TOTALS
-- ) AS js2 USING (a)
-- ORDER BY b ASC NULLS FIRST;

SELECT '---';
SELECT
    *
FROM (
    SELECT ([to_string(number % 2)] :: array(low_cardinality(string))) AS item_id, count() FROM numbers(3) GROUP BY item_id
    WITH TOTALS
) AS l
 FULL JOIN (
    SELECT ([to_string((number % 2) * 2)] :: array(string)) AS item_id FROM numbers(3)
) AS r
ON l.item_id = r.item_id
ORDER BY item_id, `count()`, r.item_id
;

SELECT '---';
SELECT
    *
FROM (
    SELECT ([to_string(number % 2)] :: array(low_cardinality(string))) AS item_id, count() FROM numbers(3) GROUP BY item_id
    WITH TOTALS
) AS l
FULL JOIN (
    SELECT ([to_string((number % 2) * 2)] :: array(string)) AS item_id, count() FROM numbers(3) GROUP BY item_id
    WITH TOTALS
) AS r
ON l.item_id = r.item_id
ORDER BY item_id, `count()`, r.item_id, `r.count()`
;

SELECT '---';
SELECT
    *
FROM (
    SELECT ([to_string(number % 2)] :: array(string)) AS item_id FROM numbers(3)
) AS l
FULL JOIN (
    SELECT ([to_string((number % 2) * 2)] :: array(low_cardinality(string))) AS item_id, count() FROM numbers(3) GROUP BY item_id
    WITH TOTALS
) AS r
ON l.item_id = r.item_id
ORDER BY item_id, r.item_id, `count()`
;
