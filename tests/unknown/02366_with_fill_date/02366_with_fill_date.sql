-- Tags: no-backward-compatibility-check
set query_mode='table';
SELECT to_date('2022-02-01') AS d1
FROM numbers(18) AS number
ORDER BY d1 ASC WITH FILL FROM to_datetime('2022-02-01') TO to_datetime('2022-07-01') STEP to_interval_month(1); -- { serverError 475 }

