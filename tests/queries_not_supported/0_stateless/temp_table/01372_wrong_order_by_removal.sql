CREATE TEMPORARY STREAM moving_sum_num
(
    `k` string,
    `dt` DateTime,
    `v` uint64
);

-- ORDER BY from subquery shall not be removed.
EXPLAIN SYNTAX SELECT k, groupArrayMovingSum(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k;
