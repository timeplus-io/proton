WITH to_datetime('2020-06-16 03:00:00') AS date_time
SELECT date_time ORDER BY date_time ASC
WITH FILL
    FROM to_datetime('2020-06-16 00:00:00')
    TO to_datetime('2020-06-16 10:00:00')
    STEP 1800;
