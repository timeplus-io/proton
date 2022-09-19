SELECT to_start_of_day(to_datetime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT to_start_of_day(to_datetime('2017-12-31 03:45:00', 'UTC'), 'UTC'); -- success

SELECT to_start_of_month(to_datetime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT to_start_of_month(to_datetime('2017-12-31 00:00:00', 'UTC'), 'UTC'); -- success

SELECT to_start_of_quarter(to_datetime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT to_start_of_quarter(to_datetime('2017-12-31 00:00:00', 'UTC'), 'UTC'); -- success

SELECT to_start_of_year(to_datetime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT to_start_of_year(to_datetime('2017-12-31 00:00:00', 'UTC'), 'UTC'); -- success

SELECT to_start_of_ten_minute(to_datetime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT to_start_of_ten_minute(to_datetime('2017-12-31 05:12:30', 'UTC'), 'UTC'); -- success

SELECT to_start_of_fifteen_minute(to_datetime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT to_start_of_fifteen_minute(to_datetime('2017-12-31 01:17:00', 'UTC'), 'UTC'); -- success

SELECT to_start_of_hour(to_datetime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT to_start_of_hour(to_datetime('2017-12-31 01:59:00', 'UTC'), 'UTC'); -- success

SELECT to_start_of_minute(to_datetime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT to_start_of_minute(to_datetime('2017-12-31 00:01:30', 'UTC'), 'UTC'); -- success

-- special case - allow empty time_zone when using functions like today(), yesterday() etc.
SELECT to_start_of_day(today()) FORMAT Null; -- success
SELECT to_start_of_day(yesterday()) FORMAT Null; -- success
