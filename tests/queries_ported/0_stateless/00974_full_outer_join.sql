SELECT
    q0.dt,
    q0.cnt,
    q1.cnt2
FROM
(
    SELECT
        to_date(add_days(to_date('2015-12-01'), number)) AS dt,
        sum(number) AS cnt
    FROM numbers(2)
    GROUP BY dt
) AS q0
ALL FULL OUTER JOIN
(
    SELECT
        to_date(add_days(to_date('2015-12-01'), number)) AS dt,
        sum(number) AS cnt2
    FROM numbers(5)
    GROUP BY dt
) AS q1 ON q0.dt = q1.dt
ORDER BY q1.cnt2;
