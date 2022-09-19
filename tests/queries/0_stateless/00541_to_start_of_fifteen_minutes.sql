SELECT
    DISTINCT result 
FROM (
    SELECT
        to_start_of_fifteen_minute(to_datetime('2017-12-25 00:00:00') + number * 60) AS result
    FROM system.numbers
    LIMIT 120
) ORDER BY result
