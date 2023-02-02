SELECT sign(0);
SELECT sign(1);
SELECT sign(-1);

DROP STREAM IF EXISTS test;

CREATE STREAM test(
	n1 int32,
	n2 uint32,
	n3 float32,
	n4 float64,
	n5 decimal32(5)
) ENGINE = Memory;

INSERT INTO test VALUES (1, 2, -0.0001, 1.5, 0.5) (-2, 0, 2.5, -4, -5) (4, 5, 5, 0, 7);

SELECT 'sign(int32)';
SELECT sign(n1) FROM test;

SELECT 'sign(uint32)';
SELECT sign(n2) FROM test;

SELECT 'sign(float32)';
SELECT sign(n3) FROM test;

SELECT 'sign(float64)';
SELECT sign(n4) FROM test;

SELECT 'sign(decimal32(5))';
SELECT sign(n5) FROM test;

DROP STREAM test;
