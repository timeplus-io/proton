SELECT CAST('[1, 2, 3]' AS array(uint8));
SELECT CAST(to_string(range(number)) AS array(uint64)), CAST(to_string((number, number * 111)) AS tuple(uint64, uint8)) FROM system.numbers LIMIT 10;
