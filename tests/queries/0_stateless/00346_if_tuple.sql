SELECT number % 3 = 2 ? (number, to_string(number)) : (number * 10, concat('! ', to_string(number))) FROM system.numbers LIMIT 10;

SELECT 0 ? (number, to_string(number)) : (number * 10, concat('! ', to_string(number))) FROM system.numbers LIMIT 10;
SELECT 1 ? (number, to_string(number)) : (number * 10, concat('! ', to_string(number))) FROM system.numbers LIMIT 10;

SELECT number % 3 = 2 ? (1, 'Hello') : (2, 'World') FROM system.numbers LIMIT 10;
SELECT number % 3 = 2 ? (number, 'Hello') : (0, 'World') FROM system.numbers LIMIT 10;
SELECT number % 3 = 2 ? (number, 'Hello') : (0, to_string(exp2(number))) FROM system.numbers LIMIT 10;
