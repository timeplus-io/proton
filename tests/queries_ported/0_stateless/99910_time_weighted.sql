SELECT
    round(avg_time_weighted(v, t), 6),
    round(avg_time_weighted(v, t, to_datetime('2024-01-01 00:00:09')), 6)
FROM
(
    SELECT *
    FROM
    (
        SELECT to_int32(1) AS v, to_datetime('2024-01-01 00:00:00') AS t
        UNION ALL
        SELECT to_int32(3) AS v, to_datetime('2024-01-01 00:00:03') AS t
        UNION ALL
        SELECT to_int32(5) AS v, to_datetime('2024-01-01 00:00:05') AS t
    )
    ORDER BY t
);

SELECT
    round(avg_time_weighted(v, t64), 6),
    round(avg_time_weighted(v, t64, to_datetime64('2024-01-01 00:00:03.000', 3)), 6)
FROM
(
    SELECT *
    FROM
    (
        SELECT to_int32(1) AS v, to_datetime64('2024-01-01 00:00:00.000', 3) AS t64
        UNION ALL
        SELECT to_int32(3) AS v, to_datetime64('2024-01-01 00:00:00.500', 3) AS t64
        UNION ALL
        SELECT to_int32(5) AS v, to_datetime64('2024-01-01 00:00:02.000', 3) AS t64
    )
    ORDER BY t64
);

SELECT round(avg_time_weighted(v, t), 6)
FROM
(
    SELECT *
    FROM
    (
        SELECT to_int32(-100) AS v, to_datetime('2024-01-01 00:00:00') AS t
        UNION ALL
        SELECT to_int32(3) AS v, to_datetime('2024-01-01 00:00:03') AS t
        UNION ALL
        SELECT to_int32(5) AS v, to_datetime('2024-01-01 00:00:05') AS t
    )
    ORDER BY t
);

SELECT round(avg_time_weighted(v, t, to_datetime('2024-01-01 00:00:09')), 6)
FROM
(
    SELECT *
    FROM
    (
        SELECT to_int32(-100) AS v, to_datetime('2024-01-01 00:00:00') AS t
        UNION ALL
        SELECT to_int32(3) AS v, to_datetime('2024-01-01 00:00:03') AS t
        UNION ALL
        SELECT to_int32(5) AS v, to_datetime('2024-01-01 00:00:05') AS t
    )
    ORDER BY t
);

SELECT round(avg_time_weighted(v, t), 6)
FROM
(
    SELECT *
    FROM
    (
        SELECT to_float64(1.5) AS v, to_datetime('2024-01-01 00:00:00') AS t
        UNION ALL
        SELECT to_float64(2.5) AS v, to_datetime('2024-01-01 00:00:02') AS t
        UNION ALL
        SELECT to_float64(10) AS v, to_datetime('2024-01-01 00:00:04') AS t
    )
    ORDER BY t
);

SELECT avg_time_weighted(val, a), avg_time_weighted(val, a, to_datetime('2024-11-29 12:12:28'))
FROM
(
    SELECT *
    FROM
    (
        SELECT to_int32(1) AS val, to_datetime('2024-11-29 12:12:13') AS a, to_date('2024-11-29') AS b, to_date32('2024-11-29') AS c, to_datetime64('2024-11-29 12:12:13.123', 3) AS d
        UNION ALL
        SELECT to_int32(2) AS val, to_datetime('2024-11-29 12:12:16') AS a, to_date('2024-11-30') AS b, to_date32('2024-11-30') AS c, to_datetime64('2024-11-29 12:12:13.126', 3) AS d
        UNION ALL
        SELECT to_int32(3) AS val, to_datetime('2024-11-29 12:12:17') AS a, to_date('2024-12-01') AS b, to_date32('2024-12-01') AS c, to_datetime64('2024-11-29 12:12:13.127', 3) AS d
        UNION ALL
        SELECT to_int32(4) AS val, to_datetime('2024-11-29 12:12:18') AS a, to_date('2024-12-03') AS b, to_date32('2024-12-03') AS c, to_datetime64('2024-11-29 12:12:13.128', 3) AS d
        UNION ALL
        SELECT to_int32(5) AS val, to_datetime('2024-11-29 12:12:19') AS a, to_date('2024-12-28') AS b, to_date32('2024-12-28') AS c, to_datetime64('2024-11-29 12:12:13.129', 3) AS d
        UNION ALL
        SELECT to_int32(6) AS val, to_datetime('2024-11-29 12:12:25') AS a, to_date('2024-12-29') AS b, to_date32('2024-12-29') AS c, to_datetime64('2024-11-29 12:12:13.135', 3) AS d
    )
    ORDER BY a
);

SELECT avg_time_weighted(val, b), avg_time_weighted(val, b, to_date('2025-01-08'))
FROM
(
    SELECT *
    FROM
    (
        SELECT to_int32(1) AS val, to_datetime('2024-11-29 12:12:13') AS a, to_date('2024-11-29') AS b, to_date32('2024-11-29') AS c, to_datetime64('2024-11-29 12:12:13.123', 3) AS d
        UNION ALL
        SELECT to_int32(2) AS val, to_datetime('2024-11-29 12:12:16') AS a, to_date('2024-11-30') AS b, to_date32('2024-11-30') AS c, to_datetime64('2024-11-29 12:12:13.126', 3) AS d
        UNION ALL
        SELECT to_int32(3) AS val, to_datetime('2024-11-29 12:12:17') AS a, to_date('2024-12-01') AS b, to_date32('2024-12-01') AS c, to_datetime64('2024-11-29 12:12:13.127', 3) AS d
        UNION ALL
        SELECT to_int32(4) AS val, to_datetime('2024-11-29 12:12:18') AS a, to_date('2024-12-03') AS b, to_date32('2024-12-03') AS c, to_datetime64('2024-11-29 12:12:13.128', 3) AS d
        UNION ALL
        SELECT to_int32(5) AS val, to_datetime('2024-11-29 12:12:19') AS a, to_date('2024-12-28') AS b, to_date32('2024-12-28') AS c, to_datetime64('2024-11-29 12:12:13.129', 3) AS d
        UNION ALL
        SELECT to_int32(6) AS val, to_datetime('2024-11-29 12:12:25') AS a, to_date('2024-12-29') AS b, to_date32('2024-12-29') AS c, to_datetime64('2024-11-29 12:12:13.135', 3) AS d
    )
    ORDER BY b
);

SELECT avg_time_weighted(val, c), avg_time_weighted(val, c, to_date32('2025-01-08'))
FROM
(
    SELECT *
    FROM
    (
        SELECT to_int32(1) AS val, to_datetime('2024-11-29 12:12:13') AS a, to_date('2024-11-29') AS b, to_date32('2024-11-29') AS c, to_datetime64('2024-11-29 12:12:13.123', 3) AS d
        UNION ALL
        SELECT to_int32(2) AS val, to_datetime('2024-11-29 12:12:16') AS a, to_date('2024-11-30') AS b, to_date32('2024-11-30') AS c, to_datetime64('2024-11-29 12:12:13.126', 3) AS d
        UNION ALL
        SELECT to_int32(3) AS val, to_datetime('2024-11-29 12:12:17') AS a, to_date('2024-12-01') AS b, to_date32('2024-12-01') AS c, to_datetime64('2024-11-29 12:12:13.127', 3) AS d
        UNION ALL
        SELECT to_int32(4) AS val, to_datetime('2024-11-29 12:12:18') AS a, to_date('2024-12-03') AS b, to_date32('2024-12-03') AS c, to_datetime64('2024-11-29 12:12:13.128', 3) AS d
        UNION ALL
        SELECT to_int32(5) AS val, to_datetime('2024-11-29 12:12:19') AS a, to_date('2024-12-28') AS b, to_date32('2024-12-28') AS c, to_datetime64('2024-11-29 12:12:13.129', 3) AS d
        UNION ALL
        SELECT to_int32(6) AS val, to_datetime('2024-11-29 12:12:25') AS a, to_date('2024-12-29') AS b, to_date32('2024-12-29') AS c, to_datetime64('2024-11-29 12:12:13.135', 3) AS d
    )
    ORDER BY c
);

SELECT avg_time_weighted(val, d), avg_time_weighted(val, d, to_datetime64('2024-11-29 12:12:13.138', 3))
FROM
(
    SELECT *
    FROM
    (
        SELECT to_int32(1) AS val, to_datetime('2024-11-29 12:12:13') AS a, to_date('2024-11-29') AS b, to_date32('2024-11-29') AS c, to_datetime64('2024-11-29 12:12:13.123', 3) AS d
        UNION ALL
        SELECT to_int32(2) AS val, to_datetime('2024-11-29 12:12:16') AS a, to_date('2024-11-30') AS b, to_date32('2024-11-30') AS c, to_datetime64('2024-11-29 12:12:13.126', 3) AS d
        UNION ALL
        SELECT to_int32(3) AS val, to_datetime('2024-11-29 12:12:17') AS a, to_date('2024-12-01') AS b, to_date32('2024-12-01') AS c, to_datetime64('2024-11-29 12:12:13.127', 3) AS d
        UNION ALL
        SELECT to_int32(4) AS val, to_datetime('2024-11-29 12:12:18') AS a, to_date('2024-12-03') AS b, to_date32('2024-12-03') AS c, to_datetime64('2024-11-29 12:12:13.128', 3) AS d
        UNION ALL
        SELECT to_int32(5) AS val, to_datetime('2024-11-29 12:12:19') AS a, to_date('2024-12-28') AS b, to_date32('2024-12-28') AS c, to_datetime64('2024-11-29 12:12:13.129', 3) AS d
        UNION ALL
        SELECT to_int32(6) AS val, to_datetime('2024-11-29 12:12:25') AS a, to_date('2024-12-29') AS b, to_date32('2024-12-29') AS c, to_datetime64('2024-11-29 12:12:13.135', 3) AS d
    )
    ORDER BY d
);

SELECT median_time_weighted(val, a), median_time_weighted(val, a, to_datetime('2024-11-29 12:13:28'))
FROM
(
    SELECT *
    FROM
    (
        SELECT to_int32(1) AS val, to_datetime('2024-11-29 12:12:13') AS a, to_date('2024-11-29') AS b, to_date32('2024-11-29') AS c, to_datetime64('2024-11-29 12:12:13.123', 3) AS d
        UNION ALL
        SELECT to_int32(2) AS val, to_datetime('2024-11-29 12:12:16') AS a, to_date('2024-11-30') AS b, to_date32('2024-11-30') AS c, to_datetime64('2024-11-29 12:12:13.126', 3) AS d
        UNION ALL
        SELECT to_int32(3) AS val, to_datetime('2024-11-29 12:12:17') AS a, to_date('2024-12-01') AS b, to_date32('2024-12-01') AS c, to_datetime64('2024-11-29 12:12:13.127', 3) AS d
        UNION ALL
        SELECT to_int32(4) AS val, to_datetime('2024-11-29 12:12:18') AS a, to_date('2024-12-03') AS b, to_date32('2024-12-03') AS c, to_datetime64('2024-11-29 12:12:13.128', 3) AS d
        UNION ALL
        SELECT to_int32(5) AS val, to_datetime('2024-11-29 12:12:19') AS a, to_date('2024-12-28') AS b, to_date32('2024-12-28') AS c, to_datetime64('2024-11-29 12:12:13.129', 3) AS d
        UNION ALL
        SELECT to_int32(6) AS val, to_datetime('2024-11-29 12:12:25') AS a, to_date('2024-12-29') AS b, to_date32('2024-12-29') AS c, to_datetime64('2024-11-29 12:12:13.135', 3) AS d
    )
    ORDER BY a
);

SELECT median_time_weighted(val, b), median_time_weighted(val, b, to_date('2025-02-07'))
FROM
(
    SELECT *
    FROM
    (
        SELECT to_int32(1) AS val, to_datetime('2024-11-29 12:12:13') AS a, to_date('2024-11-29') AS b, to_date32('2024-11-29') AS c, to_datetime64('2024-11-29 12:12:13.123', 3) AS d
        UNION ALL
        SELECT to_int32(2) AS val, to_datetime('2024-11-29 12:12:16') AS a, to_date('2024-11-30') AS b, to_date32('2024-11-30') AS c, to_datetime64('2024-11-29 12:12:13.126', 3) AS d
        UNION ALL
        SELECT to_int32(3) AS val, to_datetime('2024-11-29 12:12:17') AS a, to_date('2024-12-01') AS b, to_date32('2024-12-01') AS c, to_datetime64('2024-11-29 12:12:13.127', 3) AS d
        UNION ALL
        SELECT to_int32(4) AS val, to_datetime('2024-11-29 12:12:18') AS a, to_date('2024-12-03') AS b, to_date32('2024-12-03') AS c, to_datetime64('2024-11-29 12:12:13.128', 3) AS d
        UNION ALL
        SELECT to_int32(5) AS val, to_datetime('2024-11-29 12:12:19') AS a, to_date('2024-12-28') AS b, to_date32('2024-12-28') AS c, to_datetime64('2024-11-29 12:12:13.129', 3) AS d
        UNION ALL
        SELECT to_int32(6) AS val, to_datetime('2024-11-29 12:12:25') AS a, to_date('2024-12-29') AS b, to_date32('2024-12-29') AS c, to_datetime64('2024-11-29 12:12:13.135', 3) AS d
    )
    ORDER BY b
);

SELECT median_time_weighted(val, c), median_time_weighted(val, c, to_date32('2025-02-07'))
FROM
(
    SELECT *
    FROM
    (
        SELECT to_int32(1) AS val, to_datetime('2024-11-29 12:12:13') AS a, to_date('2024-11-29') AS b, to_date32('2024-11-29') AS c, to_datetime64('2024-11-29 12:12:13.123', 3) AS d
        UNION ALL
        SELECT to_int32(2) AS val, to_datetime('2024-11-29 12:12:16') AS a, to_date('2024-11-30') AS b, to_date32('2024-11-30') AS c, to_datetime64('2024-11-29 12:12:13.126', 3) AS d
        UNION ALL
        SELECT to_int32(3) AS val, to_datetime('2024-11-29 12:12:17') AS a, to_date('2024-12-01') AS b, to_date32('2024-12-01') AS c, to_datetime64('2024-11-29 12:12:13.127', 3) AS d
        UNION ALL
        SELECT to_int32(4) AS val, to_datetime('2024-11-29 12:12:18') AS a, to_date('2024-12-03') AS b, to_date32('2024-12-03') AS c, to_datetime64('2024-11-29 12:12:13.128', 3) AS d
        UNION ALL
        SELECT to_int32(5) AS val, to_datetime('2024-11-29 12:12:19') AS a, to_date('2024-12-28') AS b, to_date32('2024-12-28') AS c, to_datetime64('2024-11-29 12:12:13.129', 3) AS d
        UNION ALL
        SELECT to_int32(6) AS val, to_datetime('2024-11-29 12:12:25') AS a, to_date('2024-12-29') AS b, to_date32('2024-12-29') AS c, to_datetime64('2024-11-29 12:12:13.135', 3) AS d
    )
    ORDER BY c
);

SELECT median_time_weighted(val, d), median_time_weighted(val, d, to_datetime64('2024-11-29 12:12:14.138', 3))
FROM
(
    SELECT *
    FROM
    (
        SELECT to_int32(1) AS val, to_datetime('2024-11-29 12:12:13') AS a, to_date('2024-11-29') AS b, to_date32('2024-11-29') AS c, to_datetime64('2024-11-29 12:12:13.123', 3) AS d
        UNION ALL
        SELECT to_int32(2) AS val, to_datetime('2024-11-29 12:12:16') AS a, to_date('2024-11-30') AS b, to_date32('2024-11-30') AS c, to_datetime64('2024-11-29 12:12:13.126', 3) AS d
        UNION ALL
        SELECT to_int32(3) AS val, to_datetime('2024-11-29 12:12:17') AS a, to_date('2024-12-01') AS b, to_date32('2024-12-01') AS c, to_datetime64('2024-11-29 12:12:13.127', 3) AS d
        UNION ALL
        SELECT to_int32(4) AS val, to_datetime('2024-11-29 12:12:18') AS a, to_date('2024-12-03') AS b, to_date32('2024-12-03') AS c, to_datetime64('2024-11-29 12:12:13.128', 3) AS d
        UNION ALL
        SELECT to_int32(5) AS val, to_datetime('2024-11-29 12:12:19') AS a, to_date('2024-12-28') AS b, to_date32('2024-12-28') AS c, to_datetime64('2024-11-29 12:12:13.129', 3) AS d
        UNION ALL
        SELECT to_int32(6) AS val, to_datetime('2024-11-29 12:12:25') AS a, to_date('2024-12-29') AS b, to_date32('2024-12-29') AS c, to_datetime64('2024-11-29 12:12:13.135', 3) AS d
    )
    ORDER BY d
);
