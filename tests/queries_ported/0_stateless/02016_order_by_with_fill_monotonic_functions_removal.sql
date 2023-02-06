SELECT to_start_of_minute(some_time) AS ts
FROM
(
    SELECT to_datetime('2021-07-07 15:21:05') AS some_time
)
ORDER BY ts ASC WITH FILL FROM to_datetime('2021-07-07 15:21:00') TO to_datetime('2021-07-07 15:21:15') STEP 5;
