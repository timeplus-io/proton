SET optimize_move_functions_out_of_any = 1;

EXPLAIN SYNTAX SELECT any(number + number * 2) FROM numbers(1, 2);
SELECT any(number + number * 2) FROM numbers(1, 2);

EXPLAIN SYNTAX SELECT any_last(number + number * 2) FROM numbers(1, 2);
SELECT any_last(number + number * 2) FROM numbers(1, 2);

EXPLAIN SYNTAX WITH any(number * 3) AS x SELECT x FROM numbers(1, 2);
WITH any(number * 3) AS x SELECT x FROM numbers(1, 2);

EXPLAIN SYNTAX SELECT any_last(number * 3) AS x, x FROM numbers(1, 2);
SELECT any_last(number * 3) AS x, x FROM numbers(1, 2);

SELECT any(any_last(number)) FROM numbers(1); -- { serverError 184 }

SET optimize_move_functions_out_of_any = 0;

EXPLAIN SYNTAX SELECT any(number + number * 2) FROM numbers(1, 2);
SELECT any(number + number * 2) FROM numbers(1, 2);

EXPLAIN SYNTAX SELECT any_last(number + number * 2) FROM numbers(1, 2);
SELECT any_last(number + number * 2) FROM numbers(1, 2);

EXPLAIN SYNTAX WITH any(number * 3) AS x SELECT x FROM numbers(1, 2);
WITH any(number * 3) AS x SELECT x FROM numbers(1, 2);

EXPLAIN SYNTAX SELECT any_last(number * 3) AS x, x FROM numbers(1, 2);
SELECT any_last(number * 3) AS x, x FROM numbers(1, 2);

SELECT any(any_last(number)) FROM numbers(1); -- { serverError 184 }

SELECT 'array_join';
SELECT *, any(array_join([[], []])) FROM numbers(1) GROUP BY number;
