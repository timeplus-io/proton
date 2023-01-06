DROP STREAM IF EXISTS test;

SELECT array_cum_sum_non_negative([1, 2, 3, 4]);

SELECT array_cum_sum_non_negative([1, -5, 5, -2]);

SELECT array_difference([1, 2, 3, 4]);

SELECT array_difference([1, 7, 100, 5]);

create stream test(a array(int64), b array(float64), c array(uint64)) ENGINE=Memory;

INSERT INTO test VALUES ([1, -3, 0, 1], [1.0, 0.4, -0.1], [1, 3, 1]);

SELECT array_cum_sum_non_negative(a) FROM test;

SELECT array_cum_sum_non_negative(b) FROM test;

SELECT array_cum_sum_non_negative(c) FROM test;

SELECT array_difference(a) FROM test;

SELECT array_difference(b) FROM test;

SELECT array_difference(c) FROM test;

DROP STREAM IF EXISTS test;

