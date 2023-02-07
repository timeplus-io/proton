-- https://github.com/ClickHouse/ClickHouse/issues/41470
SELECT
    roundBankers(100),
    -9223372036854775808,
    roundBankers(result.2, 256)
FROM
    (
        SELECT student_ttest(sample, variant) AS result
        FROM
            (
                SELECT
                        to_float64(number) % NULL AS sample,
                        0 AS variant
                FROM system.numbers
                LIMIT 1025
                UNION ALL
                SELECT
                        (to_float64(number) % 9223372036854775807) + nan AS sample,
                        -9223372036854775808 AS variant
                FROM system.numbers
                LIMIT 1024
            )
    )
FORMAT CSV
