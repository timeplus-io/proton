SELECT
    round(avg_time_weighted_merge(s), 6),
    round(avg_time_weighted_merge(s_with_end), 6)
FROM
(
    SELECT
        2 AS part,
        avg_time_weighted_state(v, t) AS s,
        avg_time_weighted_state(v, t, to_datetime('2024-01-01 00:00:09')) AS s_with_end
    FROM
    (
        SELECT *
        FROM
        (
            SELECT to_int32(5) AS v, to_datetime('2024-01-01 00:00:05') AS t
        )
        ORDER BY t
    )
    UNION ALL
    SELECT
        1 AS part,
        avg_time_weighted_state(v, t) AS s,
        avg_time_weighted_state(v, t, to_datetime('2024-01-01 00:00:09')) AS s_with_end
    FROM
    (
        SELECT *
        FROM
        (
            SELECT to_int32(1) AS v, to_datetime('2024-01-01 00:00:00') AS t
            UNION ALL
            SELECT to_int32(3) AS v, to_datetime('2024-01-01 00:00:03') AS t
        )
        ORDER BY t
    )
    ORDER BY part DESC
);
