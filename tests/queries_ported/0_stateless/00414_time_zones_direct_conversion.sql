SELECT 1301146200 + 1800 * number AS ts, to_string(to_datetime(ts), 'Australia/Sydney') AS time_in_sydney FROM system.numbers LIMIT 7;
