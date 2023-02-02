SELECT
    to_start_of_hour(c1) AS _c1,
    c2
FROM values((to_datetime('2020-01-01 01:01:01'), 999), (to_datetime('2020-01-01 01:01:59'), 1))
ORDER BY
    _c1 ASC,
    c2 ASC
