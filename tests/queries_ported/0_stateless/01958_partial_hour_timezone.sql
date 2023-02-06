-- Appeared in https://github.com/ClickHouse/ClickHouse/pull/26978#issuecomment-890889362
WITH to_datetime('1970-06-17 07:39:21', 'Africa/Monrovia') as t
SELECT to_unix_timestamp(t),
       timezone_offset(t),
       format_datetime(t, '%F %T', 'Africa/Monrovia'),
       to_string(t, 'Africa/Monrovia'),
       to_start_of_minute(t),
       to_start_of_five_minutes(t),
       to_start_of_fifteen_minutes(t),
       to_start_of_ten_minutes(t),
       to_start_of_hour(t),
       to_start_of_day(t),
       to_start_of_week(t),
       to_start_of_interval(t, INTERVAL 1 second),
       to_start_of_interval(t, INTERVAL 1 minute),
       to_start_of_interval(t, INTERVAL 2 minute),
       to_start_of_interval(t, INTERVAL 5 minute),
       to_start_of_interval(t, INTERVAL 60 minute),
       add_minutes(t, 1),
       add_minutes(t, 60)
FORMAT Vertical;
