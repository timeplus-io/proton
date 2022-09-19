SELECT to_date(toDateTime64(today(), 0, 'UTC')) = to_date(to_datetime(today(), 'UTC'));
