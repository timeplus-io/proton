SELECT min(ts = to_uint32(to_datetime(to_string(ts)))) FROM (SELECT 1000000000 + 1234 * number AS ts FROM system.numbers LIMIT 1000000);
SELECT min(ts = to_uint32(to_datetime(to_string(ts)))) FROM (SELECT 10000 + 1234 * number AS ts FROM system.numbers LIMIT 1000000);
