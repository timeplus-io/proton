SET join_use_nulls = 1;

SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;


DROP STREAM IF EXISTS null_00549;
create stream null_00549 (k uint64, a string, b nullable(string))  ;

INSERT INTO null_00549(k,a,b) SELECT
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
SELECT sleep(3);
SELECT * FROM null_00549 ORDER BY k, a, b;

DROP STREAM null_00549;
