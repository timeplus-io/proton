select to_unix_timestamp64_milli(to_datetime64('1969-12-31 23:59:59.999', 3, 'Europe/Amsterdam'));
select to_unix_timestamp64_milli(to_datetime64('1969-12-31 23:59:59.999', 3, 'UTC'));
select from_unix_timestamp64_milli(to_int64(-1), 'Europe/Amsterdam');
select from_unix_timestamp64_milli(to_int64(-1), 'UTC');
