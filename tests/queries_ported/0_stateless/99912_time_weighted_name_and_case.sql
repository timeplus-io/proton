SELECT
    position(to_type_name(avg_time_weighted_state(to_int32(1), to_datetime('2024-01-01 00:00:00'))), 'avg_time_weighted') > 0,
    position(to_type_name(median_time_weighted_state(to_int32(1), to_datetime('2024-01-01 00:00:00'))), 'median_time_weighted') > 0;

SELECT
    round(AVG_time_weighted(v, t), 6) = round(avg_time_weighted(v, t), 6),
    round(MeDiAn_time_weighted(v, t), 6) = round(median_time_weighted(v, t), 6)
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
