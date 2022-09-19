SET join_use_nulls = 1;

DROP STREAM IF EXISTS null_00549;
create stream null_00549 (k uint64, a string, b Nullable(string))  ;

INSERT INTO null_00549 SELECT
    k,
    a,
    b
FROM
(
    SELECT
        number AS k,
        to_string(number) AS a
    FROM system.numbers
    LIMIT 2
) js1
ANY LEFT JOIN
(
    SELECT
        number AS k,
        to_string(number) AS b
    FROM system.numbers
    LIMIT 1, 2
) js2 USING (k)
ORDER BY k ASC;

SELECT * FROM null_00549 ORDER BY k, a, b;

DROP STREAM null_00549;
