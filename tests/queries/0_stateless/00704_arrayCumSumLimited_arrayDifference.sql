DROP STREAM IF EXISTS test;

SELECT arrayCumSumNonNegative([1, 2, 3, 4]);

SELECT arrayCumSumNonNegative([1, -5, 5, -2]);

SELECT arrayDifference([1, 2, 3, 4]);

SELECT arrayDifference([1, 7, 100, 5]);

create stream test(a array(int64), b array(float64), c array(uint64)) ENGINE=Memory;

INSERT INTO test VALUES ([1, -3, 0, 1], [1.0, 0.4, -0.1], [1, 3, 1]);

SELECT arrayCumSumNonNegative(a) FROM test;

SELECT arrayCumSumNonNegative(b) FROM test;

SELECT arrayCumSumNonNegative(c) FROM test;

SELECT arrayDifference(a) FROM test;

SELECT arrayDifference(b) FROM test;

SELECT arrayDifference(c) FROM test;

DROP STREAM IF EXISTS test;

