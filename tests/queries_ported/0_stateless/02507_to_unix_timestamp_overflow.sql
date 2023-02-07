SELECT to_unix_timestamp(to_datetime64('1928-12-31 12:12:12.123', 3, 'UTC')); -- { serverError DECIMAL_OVERFLOW }
SELECT to_int64(to_datetime64('1928-12-31 12:12:12.123', 3, 'UTC'));
