SELECT to_type_name(sum(n)) FROM (SELECT to_uint16(number) AS n FROM system.numbers LIMIT 100);
SELECT to_type_name(sumWithOverflow(n)) FROM (SELECT to_uint16(number) AS n FROM system.numbers LIMIT 100);
SELECT to_type_name(sum(n)) FROM (SELECT to_float32(number) AS n FROM system.numbers LIMIT 100);
SELECT to_type_name(sumWithOverflow(n)) FROM (SELECT to_float32(number) AS n FROM system.numbers LIMIT 100);

SELECT sum(n) FROM (SELECT to_uint16(number) AS n FROM system.numbers LIMIT 100);
SELECT sumWithOverflow(n) FROM (SELECT to_uint16(number) AS n FROM system.numbers LIMIT 100);
