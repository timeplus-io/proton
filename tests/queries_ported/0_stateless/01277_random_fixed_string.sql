SELECT random_fixed_string('string'); -- { serverError 43 }
SELECT random_fixed_string(0); -- { serverError 69 }
SELECT random_fixed_string(rand() % 10); -- { serverError 44 }
SELECT to_type_name(random_fixed_string(10));
SELECT DISTINCT c > 30000 FROM (SELECT array_join(array_map(x -> reinterpret_as_uint8(substring(random_fixed_string(100), x + 1, 1)), range(100))) AS byte, count() AS c FROM numbers(100000) GROUP BY byte ORDER BY byte);
