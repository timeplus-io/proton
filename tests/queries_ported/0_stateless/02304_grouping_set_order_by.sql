SELECT to_start_of_hour(time) as timex, id, count()
FROM
(
    SELECT
        concat('id', to_string(number % 3)) AS id,
        to_datetime('2020-01-01') + (number * 60) AS time
    FROM numbers(100)
)
GROUP BY
    GROUPING SETS ( (timex, id), (timex))
ORDER BY timex ASC, id;
