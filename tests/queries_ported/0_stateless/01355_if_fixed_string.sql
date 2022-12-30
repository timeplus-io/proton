SELECT if(number % 2, to_fixed_string(to_string(number), 2), to_fixed_string(to_string(-number), 5)) AS x, to_type_name(x) FROM system.numbers LIMIT 10;
SELECT if(number % 2, to_fixed_string(to_string(number), 2), to_fixed_string(to_string(-number), 2)) AS x, to_type_name(x) FROM system.numbers LIMIT 10;

SELECT multi_if(number % 2, to_fixed_string(to_string(number), 2), to_fixed_string(to_string(-number), 5)) AS x, to_type_name(x) FROM system.numbers LIMIT 10;
SELECT multi_if(number % 2, to_fixed_string(to_string(number), 2), to_fixed_string(to_string(-number), 2)) AS x, to_type_name(x) FROM system.numbers LIMIT 10;
