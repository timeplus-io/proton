SELECT CAST('a', 'nullable(fixed_string(1))') as s,  to_type_name(s), to_string(s);
SELECT number, to_type_name(s), to_string(s) FROM (SELECT number, if(number % 3 = 0, NULL, to_fixed_string(to_string(number), 1)) AS s from numbers(10)) ORDER BY number;
