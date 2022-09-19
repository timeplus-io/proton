SELECT randomFixedString('string'); -- { serverError 43 }
SELECT randomFixedString(0); -- { serverError 69 }
SELECT randomFixedString(rand() % 10); -- { serverError 44 }
SELECT to_type_name(randomFixedString(10));
SELECT DISTINCT c > 30000 FROM (SELECT array_join(array_map(x -> reinterpret_as_uint8(substring(randomFixedString(100), x + 1, 1)), range(100))) AS byte, count() AS c FROM numbers(100000) GROUP BY byte ORDER BY byte);
