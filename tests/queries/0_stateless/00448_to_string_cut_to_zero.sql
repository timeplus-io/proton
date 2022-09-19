SELECT DISTINCT to_string(number) = toStringCutToZero(to_string(number)) FROM (SELECT * FROM system.numbers LIMIT 1000);
SELECT DISTINCT to_string(number) = toStringCutToZero(to_fixed_string(to_string(number), 10)) FROM (SELECT * FROM system.numbers LIMIT 1000);
