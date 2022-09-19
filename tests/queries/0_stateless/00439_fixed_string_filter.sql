SELECT DISTINCT x FROM (SELECT to_fixed_string(number < 20 ? '' : 'Hello', 5) AS x FROM system.numbers LIMIT 50) WHERE x != '\0\0\0\0\0';
