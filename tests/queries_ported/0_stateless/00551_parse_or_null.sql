SELECT to_int64_or_zero(s), to_uint64_or_null(s) FROM (SELECT CASE WHEN number % 2 = 1 THEN to_string(number) ELSE 'hello' END AS s FROM system.numbers) LIMIT 10;
SELECT to_int64_or_zero(s), to_uint64_or_null(s) FROM (SELECT CASE WHEN number = 5 THEN NULL WHEN number % 2 = 1 THEN to_string(number) ELSE 'hello' END AS s FROM system.numbers) LIMIT 10;
