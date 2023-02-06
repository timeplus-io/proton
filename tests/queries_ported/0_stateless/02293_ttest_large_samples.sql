SELECT round_bankers(result.1, 5), round_bankers(result.2, 5) FROM (
SELECT
     student_ttest(sample, variant) as result
FROM (
SELECT
    to_float64(number) % 30 AS sample,
    0 AS variant
FROM system.numbers limit 500000

UNION ALL

SELECT
    to_float64(number) % 30 + 0.0022 AS sample,
    1 AS variant
FROM system.numbers limit 500000));


SELECT round_bankers(result.1, 5), round_bankers(result.2, 5 ) FROM (
SELECT
     student_ttest(sample, variant) as result
FROM (
SELECT
    to_float64(number) % 30 AS sample,
    0 AS variant
FROM system.numbers limit 50000000

UNION ALL

SELECT
    to_float64(number) % 30 + 0.0022 AS sample,
    1 AS variant
FROM system.numbers limit 50000000));


SELECT round_bankers(result.2, 1025)
FROM
(
    SELECT student_ttest(sample, variant) AS result
    FROM
    (
        SELECT
            to_float64(number) % 30 AS sample,
            1048576 AS variant
        FROM system.numbers
        LIMIT 1
        UNION ALL
        SELECT
            (to_float64(number) % 7) + inf AS sample,
            255 AS variant
        FROM system.numbers
        LIMIT 1023
    )
); -- { serverError 36 }
