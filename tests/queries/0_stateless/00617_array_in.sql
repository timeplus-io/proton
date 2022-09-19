DROP STREAM IF EXISTS test_array_ops;
create stream test_array_ops(arr array(Nullable(int64))) ;

INSERT INTO test_array_ops(arr) values ([null, 10, -20]);
INSERT INTO test_array_ops(arr) values ([10, -20]);
INSERT INTO test_array_ops(arr) values ([]);

SELECT count(*) FROM test_array_ops where arr < CAST([10, -20] AS array(Nullable(int64)));
SELECT count(*) FROM test_array_ops where arr > CAST([10, -20] AS array(Nullable(int64)));
SELECT count(*) FROM test_array_ops where arr >= CAST([10, -20] AS array(Nullable(int64)));
SELECT count(*) FROM test_array_ops where arr <= CAST([10, -20] AS array(Nullable(int64)));
SELECT count(*) FROM test_array_ops where arr = CAST([10, -20] AS array(Nullable(int64)));
SELECT count(*) FROM test_array_ops where arr IN( CAST([10, -20] AS array(Nullable(int64))), CAST([null,10, -20] AS array(Nullable(int64))));

DROP STREAM test_array_ops;
