CREATE TEMPORARY STREAM test_float (x float64);
INSERT INTO test_float FORMAT TabSeparated 1.075e+06
SELECT * FROM test_float;
