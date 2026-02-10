-- Test lambda expressions as function arguments (issue #9688)
-- Lambda expressions should work with or without parentheses after the fix

-- Single-param lambda after non-identifier argument (the original bug)
SELECT 'Test 1: Lambda as second argument without parentheses';
SELECT to_type_name(random_in_type('uint64', x -> x + 1));

SELECT 'Test 2: Lambda as third argument without parentheses';
SELECT to_type_name(random_in_type('uint64', 10, x -> x + 1));

SELECT 'Test 3: Lambda with parentheses (still supported)';
SELECT to_type_name(random_in_type('uint64', (x -> x + 1)));

-- Single-param lambda as first argument (baseline, must not regress)
SELECT 'Test 4: Single-param lambda as first arg';
SELECT array_map(x -> x + 1, [1, 2, 3]);

SELECT 'Test 5: Lambda in nested function call';
SELECT array_map(x -> x * 2, array_map(y -> y + 1, [1, 2, 3]));

SELECT 'Test 6: Lambda with complex expression';
SELECT to_type_name(random_in_type('uint32', x -> to_uint32(x + 10)));

-- Multi-param lambda without parentheses (x, y are both lambda params)
SELECT 'Test 7: Multi-param lambda without parentheses';
SELECT array_map(x, y -> x + y, [1, 2, 3], [10, 20, 30]);

-- Multi-param lambda with parentheses
SELECT 'Test 8: Multi-param lambda with parentheses';
SELECT array_map((x, y) -> x + y, [1, 2, 3], [10, 20, 30]);

-- Multi-param lambda with parentheses after non-identifier arg
SELECT 'Test 9: Multi-param lambda with parentheses after non-identifier arg';
SELECT array_first((x, y) -> x > y, [1, 5, 3], [2, 4, 6]);

-- array_sort with lambda as second arg (lambda after array, a non-identifier)
SELECT 'Test 10: Lambda as second arg in array_sort';
SELECT array_sort(x -> -x, [3, 1, 2]);

-- Chained higher-order functions
SELECT 'Test 11: Chained higher-order functions';
SELECT array_filter(x -> x > 3, array_map(y -> y * 2, [1, 2, 3]));
