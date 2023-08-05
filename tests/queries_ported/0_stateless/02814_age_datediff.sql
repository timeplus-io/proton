-- { echo }

-- DateTime64 vs DateTime64 with fractional part
SELECT age('microsecond', to_datetime64('2015-08-18 20:30:36.100200005', 9, 'UTC'), to_datetime64('2015-08-18 20:30:41.200400005', 9, 'UTC'));
SELECT age('microsecond', to_datetime64('2015-08-18 20:30:36.100200005', 9, 'UTC'), to_datetime64('2015-08-18 20:30:41.200400004', 9, 'UTC'));

SELECT age('millisecond', to_datetime64('2015-08-18 20:30:36.450299', 6, 'UTC'), to_datetime64('2015-08-18 20:30:41.550299', 6, 'UTC'));
SELECT age('millisecond', to_datetime64('2015-08-18 20:30:36.450299', 6, 'UTC'), to_datetime64('2015-08-18 20:30:41.550298', 6, 'UTC'));

SELECT age('second', to_datetime64('2023-03-01 19:18:36.999003', 6, 'UTC'), to_datetime64('2023-03-01 19:18:41.999002', 6, 'UTC'));
SELECT age('second', to_datetime64('2023-03-01 19:18:36.999', 3, 'UTC'), to_datetime64('2023-03-01 19:18:41.001', 3, 'UTC'));

SELECT age('minute', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-01 20:35:36.300', 3, 'UTC'));
SELECT age('minute', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-01 20:35:36.100', 3, 'UTC'));
SELECT age('minute', to_datetime64('2015-01-01 20:30:36.200101', 6, 'UTC'), to_datetime64('2015-01-01 20:35:36.200100', 6, 'UTC'));

SELECT age('hour', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-01 23:30:36.200', 3, 'UTC'));
SELECT age('hour', to_datetime64('2015-01-01 20:31:36.200', 3, 'UTC'), to_datetime64('2015-01-01 23:30:36.200', 3, 'UTC'));
SELECT age('hour', to_datetime64('2015-01-01 20:30:37.200', 3, 'UTC'), to_datetime64('2015-01-01 23:30:36.200', 3, 'UTC'));
SELECT age('hour', to_datetime64('2015-01-01 20:30:36.300', 3, 'UTC'), to_datetime64('2015-01-01 23:30:36.200', 3, 'UTC'));
SELECT age('hour', to_datetime64('2015-01-01 20:30:36.200101', 6, 'UTC'), to_datetime64('2015-01-01 23:30:36.200100', 6, 'UTC'));

SELECT age('day', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-04 20:30:36.200', 3, 'UTC'));
SELECT age('day', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-04 19:30:36.200', 3, 'UTC'));
SELECT age('day', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-04 20:28:36.200', 3, 'UTC'));
SELECT age('day', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-04 20:30:35.200', 3, 'UTC'));
SELECT age('day', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-04 20:30:36.199', 3, 'UTC'));
SELECT age('day', to_datetime64('2015-01-01 20:30:36.200101', 6, 'UTC'), to_datetime64('2015-01-04 20:30:36.200100', 6, 'UTC'));

SELECT age('week', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-15 20:30:36.200', 3, 'UTC'));
SELECT age('week', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-15 19:30:36.200', 3, 'UTC'));
SELECT age('week', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-15 20:29:36.200', 3, 'UTC'));
SELECT age('week', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-15 20:30:35.200', 3, 'UTC'));
SELECT age('week', to_datetime64('2015-01-01 20:30:36.200', 3, 'UTC'), to_datetime64('2015-01-15 20:30:36.100', 3, 'UTC'));
SELECT age('week', to_datetime64('2015-01-01 20:30:36.200101', 6, 'UTC'), to_datetime64('2015-01-15 20:30:36.200100', 6, 'UTC'));

SELECT age('month', to_datetime64('2015-01-02 20:30:36.200', 3, 'UTC'), to_datetime64('2016-05-02 20:30:36.200', 3, 'UTC'));
SELECT age('month', to_datetime64('2015-01-02 20:30:36.200', 3, 'UTC'), to_datetime64('2016-05-01 20:30:36.200', 3, 'UTC'));
SELECT age('month', to_datetime64('2015-01-02 20:30:36.200', 3, 'UTC'), to_datetime64('2016-05-02 19:30:36.200', 3, 'UTC'));
SELECT age('month', to_datetime64('2015-01-02 20:30:36.200', 3, 'UTC'), to_datetime64('2016-05-02 20:29:36.200', 3, 'UTC'));
SELECT age('month', to_datetime64('2015-01-02 20:30:36.200', 3, 'UTC'), to_datetime64('2016-05-02 20:30:35.200', 3, 'UTC'));
SELECT age('month', to_datetime64('2015-01-02 20:30:36.200', 3, 'UTC'), to_datetime64('2016-05-02 20:30:36.100', 3, 'UTC'));
SELECT age('month', to_datetime64('2015-01-02 20:30:36.200101', 6, 'UTC'), to_datetime64('2016-05-02 20:30:36.200100', 6, 'UTC'));

SELECT age('quarter', to_datetime64('2015-01-02 20:30:36.200', 3, 'UTC'), to_datetime64('2016-04-02 20:30:36.200', 3, 'UTC'));
SELECT age('quarter', to_datetime64('2015-01-02 20:30:36.200', 3, 'UTC'), to_datetime64('2016-04-01 20:30:36.200', 3, 'UTC'));
SELECT age('quarter', to_datetime64('2015-01-02 20:30:36.200', 3, 'UTC'), to_datetime64('2016-04-02 19:30:36.200', 3, 'UTC'));
SELECT age('quarter', to_datetime64('2015-01-02 20:30:36.200', 3, 'UTC'), to_datetime64('2016-04-02 20:29:36.200', 3, 'UTC'));
SELECT age('quarter', to_datetime64('2015-01-02 20:30:36.200', 3, 'UTC'), to_datetime64('2016-04-02 20:30:35.200', 3, 'UTC'));
SELECT age('quarter', to_datetime64('2015-01-02 20:30:36.200', 3, 'UTC'), to_datetime64('2016-04-02 20:30:36.100', 3, 'UTC'));
SELECT age('quarter', to_datetime64('2015-01-02 20:30:36.200101', 6, 'UTC'), to_datetime64('2016-04-02 20:30:36.200100', 6, 'UTC'));

SELECT age('year', to_datetime64('2015-02-02 20:30:36.200', 3, 'UTC'), to_datetime64('2023-02-02 20:30:36.200', 3, 'UTC'));
SELECT age('year', to_datetime64('2015-02-02 20:30:36.200', 3, 'UTC'), to_datetime64('2023-01-02 20:30:36.200', 3, 'UTC'));
SELECT age('year', to_datetime64('2015-02-02 20:30:36.200', 3, 'UTC'), to_datetime64('2023-02-01 20:30:36.200', 3, 'UTC'));
SELECT age('year', to_datetime64('2015-02-02 20:30:36.200', 3, 'UTC'), to_datetime64('2023-02-02 19:30:36.200', 3, 'UTC'));
SELECT age('year', to_datetime64('2015-02-02 20:30:36.200', 3, 'UTC'), to_datetime64('2023-02-02 20:29:36.200', 3, 'UTC'));
SELECT age('year', to_datetime64('2015-02-02 20:30:36.200', 3, 'UTC'), to_datetime64('2023-02-02 20:30:35.200', 3, 'UTC'));
SELECT age('year', to_datetime64('2015-02-02 20:30:36.200', 3, 'UTC'), to_datetime64('2023-02-02 20:30:36.100', 3, 'UTC'));
SELECT age('year', to_datetime64('2015-02-02 20:30:36.200101', 6, 'UTC'), to_datetime64('2023-02-02 20:30:36.200100', 6, 'UTC'));

-- DateTime64 vs DateTime64 with negative time
SELECT age('millisecond', to_datetime64('1969-12-31 23:59:58.001', 3, 'UTC'), to_datetime64('1970-01-01 00:00:00.350', 3, 'UTC'));
SELECT age('second', to_datetime64('1969-12-31 23:59:58.001', 3, 'UTC'), to_datetime64('1970-01-01 00:00:00.35', 3, 'UTC'));
SELECT age('second', to_datetime64('1969-12-31 23:59:50.001', 3, 'UTC'), to_datetime64('1969-12-31 23:59:55.002', 3, 'UTC'));
SELECT age('second', to_datetime64('1969-12-31 23:59:50.003', 3, 'UTC'), to_datetime64('1969-12-31 23:59:55.002', 3, 'UTC'));

SELECT date_diff(millisecond, to_date16('2021-01-01'), to_date16('2021-01-02'));
SELECT date_diff(millisecond, to_date16('2021-01-01'), to_date('2021-01-03'));
SELECT date_diff(millisecond, to_date16('2021-01-01'), to_datetime('2021-01-02 00:01:01'));
SELECT date_diff(millisecond, to_date16('2021-01-01'), to_datetime64('2021-01-02 00:00:01.299', 3));
SELECT date_diff(millisecond, to_datetime64('2021-01-01 23:59:59.299', 3), to_date16('2021-01-02'));
SELECT date_diff(millisecond, to_datetime64('2021-01-01 23:59:59.299999', 6), to_date16('2021-01-02'));
SELECT date_diff(millisecond, to_datetime64('2021-01-01 23:59:59.2', 1), to_date16('2021-01-02'));
SELECT date_diff(microsecond, to_datetime64('2021-01-01 23:59:59.899999', 6, 'UTC'), to_datetime64('2021-01-02 00:01:00.100200300', 6, 'UTC'));
SELECT date_diff(microsecond, to_datetime64('1969-12-31 23:59:59.999950', 6, 'UTC'), to_datetime64('1970-01-01 00:00:00.000010', 6, 'UTC'));
SELECT date_diff(second, to_datetime64('1969-12-31 23:59:59.123000', 6, 'UTC'), to_datetime64('1970-01-01 00:00:09.123000', 6, 'UTC'));

SELECT to_YYYYMMDDhhmmss(to_datetime64('1969-12-31 23:59:59.900', 3));