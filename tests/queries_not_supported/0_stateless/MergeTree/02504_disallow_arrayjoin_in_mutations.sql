DROP STREAM IF EXISTS test_02504;

CREATE STREAM test_02504 (`a` uint32,`b` uint32) ENGINE = MergeTree ORDER BY a;
INSERT INTO test_02504 values (1, 1) (2, 2), (3, 3);
SELECT * FROM test_02504;

ALTER STREAM test_02504 UPDATE b = 33 WHERE array_join([1, 2]) = a; -- { serverError UNEXPECTED_EXPRESSION}

DROP STREAM test_02504;