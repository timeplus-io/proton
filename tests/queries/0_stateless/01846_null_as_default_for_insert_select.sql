DROP STREAM IF EXISTS test_null_as_default;
create stream test_null_as_default (a string DEFAULT 'WORLD') ;

INSERT INTO test_null_as_default SELECT 'HELLO' UNION ALL SELECT NULL;
SELECT * FROM test_null_as_default ORDER BY a;
SELECT '';

INSERT INTO test_null_as_default SELECT NULL;
SELECT * FROM test_null_as_default ORDER BY a;
SELECT '';

DROP STREAM IF EXISTS test_null_as_default;
create stream test_null_as_default (a string DEFAULT 'WORLD', b string DEFAULT 'PEOPLE') ;

INSERT INTO test_null_as_default(a) SELECT 'HELLO' UNION ALL SELECT NULL;
SELECT * FROM test_null_as_default ORDER BY a;
SELECT '';

DROP STREAM IF EXISTS test_null_as_default;
create stream test_null_as_default (a int8, b int64 DEFAULT a + 1000) ;

INSERT INTO test_null_as_default SELECT 1, NULL UNION ALL SELECT 2, NULL;
SELECT * FROM test_null_as_default ORDER BY a;
SELECT '';

DROP STREAM IF EXISTS test_null_as_default;
create stream test_null_as_default (a int8, b int64 DEFAULT c - 500, c int32 DEFAULT a + 1000) ;

INSERT INTO test_null_as_default(a, c) SELECT 1, NULL UNION ALL SELECT 2, NULL;
SELECT * FROM test_null_as_default ORDER BY a;

DROP STREAM test_null_as_default;
