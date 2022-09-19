WITH array_join(range(2)) AS delta
SELECT
    to_date(time) + toIntervalDay(delta) AS dt
FROM
(
    SELECT to_datetime('2020.11.12 19:02:04') AS time
)
ORDER BY dt ASC;

WITH array_join([0, 1]) AS delta
SELECT
    to_date(time) + toIntervalDay(delta) AS dt
FROM
(
    SELECT to_datetime('2020.11.12 19:02:04') AS time
)
ORDER BY dt ASC;
