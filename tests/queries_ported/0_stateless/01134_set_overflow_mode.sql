SET max_block_size = 10;
SET max_rows_in_set = 20;
SET set_overflow_mode = 'throw';

SELECT array_join([5, 25]) IN (SELECT DISTINCT to_uint8(int_div(number, 10)) FROM numbers(300)); -- { serverError 191 }
SELECT array_join([5, 25]) IN (SELECT DISTINCT to_uint8(int_div(number, 10)) FROM numbers(190));
SELECT array_join([5, 25]) IN (SELECT DISTINCT to_uint8(int_div(number, 10)) FROM numbers(200));
SELECT array_join([5, 25]) IN (SELECT DISTINCT to_uint8(int_div(number, 10)) FROM numbers(210)); -- { serverError 191 }

SET set_overflow_mode = 'break';

SELECT '---';

SELECT array_join([5, 25]) IN (SELECT DISTINCT to_uint8(int_div(number, 10)) FROM numbers(300));
SELECT array_join([5, 25]) IN (SELECT DISTINCT to_uint8(int_div(number, 10)) FROM numbers(190));
SELECT array_join([5, 25]) IN (SELECT DISTINCT to_uint8(int_div(number, 10)) FROM numbers(200));
SELECT array_join([5, 25]) IN (SELECT DISTINCT to_uint8(int_div(number, 10)) FROM numbers(210));
