SELECT to_year(to_datetime64('1968-12-12 11:22:33', 0, 'UTC'));
SELECT to_int16(to_relative_week_num(to_datetime64('1960-11-30 18:00:11.999', 3, 'UTC')));
SELECT to_start_of_quarter(to_datetime64('1990-01-04 12:14:12', 0, 'UTC'));
SELECT to_unix_timestamp(to_datetime64('1900-12-12 11:22:33', 0, 'UTC')); -- { serverError 407 }
