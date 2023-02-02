SELECT byte_size(123, 456.7) AS x, is_constant(x);
SELECT byte_size(number, number + 1) AS x, is_constant(x) FROM numbers(2);
SELECT byte_size();
