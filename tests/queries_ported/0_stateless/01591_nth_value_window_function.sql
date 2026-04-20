-- Test nth_value window function
SELECT nth_value(number, 1) OVER (ORDER BY number) FROM numbers(5);
SELECT nth_value(number, 3) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM numbers(5);
SELECT nth_value(to_string(number), 2) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM numbers(5);

-- nth_value with partitioning
SELECT number, nth_value(number, 1) OVER (PARTITION BY number % 2 ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM numbers(6);

-- nth_value with offset outside frame returns default
SELECT nth_value(number, 10) OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM numbers(3);

-- Error cases
SELECT nth_value(number, 0) OVER () FROM numbers(1); -- { serverError BAD_ARGUMENTS }
SELECT nth_value(number, -1) OVER () FROM numbers(1); -- { serverError BAD_ARGUMENTS }
SELECT nth_value(number) OVER () FROM numbers(1); -- { serverError BAD_ARGUMENTS }

-- Regression: frame_start reaches blocksEnd() on FOLLOWING frames past partition end.
-- Ported from ClickHouse commit 4859108febc / test 02306_window_move_row_number_fix.
SELECT nth_value(NULL, 1048577) OVER (Rows BETWEEN 1023 FOLLOWING AND UNBOUNDED FOLLOWING);
