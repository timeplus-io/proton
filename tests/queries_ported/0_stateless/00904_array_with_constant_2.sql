SELECT array_with_constant(3, number) FROM numbers(10);
SELECT array_with_constant(number, 'Hello') FROM numbers(10);
SELECT array_with_constant(number % 3, (number % 2 <> 0) ? 'Hello' : NULL) FROM numbers(10);
SELECT array_with_constant(number, []) FROM numbers(10);
