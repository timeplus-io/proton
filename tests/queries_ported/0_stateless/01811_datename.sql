WITH
    to_date('2021-04-14') AS date_value,
    to_date('2021-04-14') AS date_32_value,
    to_datetime('2021-04-14 11:22:33') AS date_time_value,
    to_datetime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT date_name('year', date_value), date_name('year', date_32_value), date_name('year', date_time_value), date_name('year', date_time_64_value);

WITH
    to_date('2021-04-14') AS date_value,
    to_date('2021-04-14') AS date_32_value,
    to_datetime('2021-04-14 11:22:33') AS date_time_value,
    to_datetime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT date_name('quarter', date_value), date_name('quarter', date_32_value), date_name('quarter', date_time_value), date_name('quarter', date_time_64_value);

WITH
    to_date('2021-04-14') AS date_value,
    to_date('2021-04-14') AS date_32_value,
    to_datetime('2021-04-14 11:22:33') AS date_time_value,
    to_datetime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT date_name('month', date_value), date_name('month', date_32_value), date_name('month', date_time_value), date_name('month', date_time_64_value);

WITH
    to_date('2021-04-14') AS date_value,
    to_date('2021-04-14') AS date_32_value,
    to_datetime('2021-04-14 11:22:33') AS date_time_value,
    to_datetime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT date_name('day_of_year', date_value), date_name('day_of_year', date_32_value), date_name('day_of_year', date_time_value), date_name('day_of_year', date_time_64_value);

WITH
    to_date('2021-04-14') AS date_value,
    to_date('2021-04-14') AS date_32_value,
    to_datetime('2021-04-14 11:22:33') AS date_time_value,
    to_datetime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT date_name('day', date_value), date_name('day', date_32_value), date_name('day', date_time_value), date_name('day', date_time_64_value);

WITH
    to_date('2021-04-14') AS date_value,
    to_date('2021-04-14') AS date_32_value,
    to_datetime('2021-04-14 11:22:33') AS date_time_value,
    to_datetime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT date_name('week', date_value), date_name('week', date_32_value), date_name('week', date_time_value), date_name('week', date_time_64_value);

WITH
    to_date('2021-04-14') AS date_value,
    to_date('2021-04-14') AS date_32_value,
    to_datetime('2021-04-14 11:22:33') AS date_time_value,
    to_datetime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT date_name('weekday', date_value), date_name('weekday', date_32_value), date_name('weekday', date_time_value), date_name('weekday', date_time_64_value);

WITH
    to_datetime('2021-04-14 11:22:33') AS date_time_value,
    to_datetime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT date_name('hour', date_time_value), date_name('hour', date_time_64_value);

WITH
    to_datetime('2021-04-14 11:22:33') AS date_time_value,
    to_datetime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT date_name('minute', date_time_value), date_name('minute', date_time_64_value);

WITH
    to_datetime('2021-04-14 11:22:33') AS date_time_value,
    to_datetime64('2021-04-14 11:22:33', 3) AS date_time_64_value
SELECT date_name('second', date_time_value), date_name('second', date_time_64_value);

WITH
    to_datetime('2021-04-14 23:22:33', 'UTC') as date
SELECT
    date_name('weekday', date, 'UTC'),
    date_name('hour', date, 'UTC'),
    date_name('minute', date, 'UTC'),
    date_name('second', date, 'UTC');

WITH
    to_datetime('2021-04-14 23:22:33', 'UTC') as date
SELECT
    date_name('weekday', date, 'Asia/Istanbul'),
    date_name('hour', date, 'Asia/Istanbul'),
    date_name('minute', date, 'Asia/Istanbul'),
    date_name('second', date, 'Asia/Istanbul');
