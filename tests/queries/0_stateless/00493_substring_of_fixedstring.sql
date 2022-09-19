SELECT substring(to_fixed_string('hello', 16), 1, 8);
SELECT substring(to_fixed_string(materialize('hello'), 16), 1, 8);
SELECT substring(to_fixed_string(to_string(number), 16), 1, 8) FROM system.numbers LIMIT 10;
SELECT substring(to_fixed_string(to_string(number), 4), 1, 3) FROM system.numbers LIMIT 995, 10;
SELECT substring(to_fixed_string(to_string(number), 4), 1, number % 5) FROM system.numbers LIMIT 995, 10;
SELECT substring(to_fixed_string(to_string(number), 4), 1 + number % 5) FROM system.numbers LIMIT 995, 10;
SELECT substring(to_fixed_string(to_string(number), 4), 1 + number % 5, 1 + number % 3) FROM system.numbers LIMIT 995, 10;
