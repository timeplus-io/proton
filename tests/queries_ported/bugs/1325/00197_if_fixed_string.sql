SELECT number % 2 ? to_string(number) : to_string(-number) FROM system.numbers LIMIT 10;
SELECT number % 2 ? to_fixed_string(to_string(number), 2) : to_fixed_string(to_string(-number), 2) FROM system.numbers LIMIT 10;
SELECT number % 2 ? to_fixed_string(to_string(number), 2) : to_string(-number) FROM system.numbers LIMIT 10;
SELECT number % 2 ? to_string(number) : to_fixed_string(to_string(-number), 2) FROM system.numbers LIMIT 10;
SELECT number % 2 ? to_string(number) : 'Hello' FROM system.numbers LIMIT 10;
SELECT number % 2 ? 'Hello' : to_string(-number) FROM system.numbers LIMIT 10;
SELECT number % 2 ? 'Hello' : 'Goodbye' FROM system.numbers LIMIT 10;
SELECT number % 2 ? to_fixed_string(to_string(number), 2) : 'Hello' FROM system.numbers LIMIT 10;
SELECT number % 2 ? 'Hello' : to_fixed_string(to_string(-number), 2) FROM system.numbers LIMIT 10;
