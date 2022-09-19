SELECT if(1 = 0, toNullable(to_uint8(0)), NULL) AS x, to_type_name(x);
SELECT if(1 = 1, toNullable(to_uint8(0)), NULL) AS x, to_type_name(x);
SELECT if(1 = 1, NULL, toNullable(to_uint8(0))) AS x, to_type_name(x);
SELECT if(1 = 0, NULL, toNullable(to_uint8(0))) AS x, to_type_name(x);

SELECT if(to_uint8(0), NULL, toNullable(to_uint8(0))) AS x, if(x = 0, 'ok', 'fail');
SELECT if(to_uint8(1), NULL, toNullable(to_uint8(0))) AS x, if(x = 0, 'fail', 'ok');
SELECT if(to_uint8(1), toNullable(to_uint8(0)), NULL) AS x, if(x = 0, 'ok', 'fail');
SELECT if(to_uint8(0), toNullable(to_uint8(0)), NULL) AS x, if(x = 0, 'fail', 'ok');

SELECT if(x = 0, 'ok', 'fail') FROM (SELECT toNullable(to_uint8(0)) AS x);
SELECT if(x = 0, 'fail', 'ok') FROM (SELECT CAST(NULL, 'Nullable(uint8)') AS x);
SELECT if(x = 0, 'fail', 'ok') FROM (SELECT materialize(CAST(NULL, 'Nullable(uint8)')) AS x);

SELECT if(x = 0, 'ok', 'fail') FROM (SELECT if(to_uint8(1), toNullable(to_uint8(0)), NULL) AS x);
SELECT if(x = 0, 'fail', 'ok') FROM (SELECT if(to_uint8(0), toNullable(to_uint8(0)), NULL) AS x);

SELECT if(x = 0, 'ok', 'fail') FROM (SELECT if(to_uint8(0), NULL, toNullable(to_uint8(0))) AS x);
SELECT if(x = 0, 'fail', 'ok') FROM (SELECT if(to_uint8(1), NULL, toNullable(to_uint8(0))) AS x);

SELECT to_type_name(x), x, isNull(x), if(x = 0, 'fail', 'ok'), if(x = 1, 'fail', 'ok'), if(x >= 0, 'fail', 'ok')
FROM (SELECT CAST(NULL, 'Nullable(uint8)') AS x);

SELECT to_type_name(x), x, isNull(x), if(x = 0, 'fail', 'ok'), if(x = 1, 'fail', 'ok'), if(x >= 0, 'fail', 'ok')
FROM (SELECT materialize(CAST(NULL, 'Nullable(uint8)')) AS x);

SELECT to_type_name(x), x, isNull(x), if(x = 0, 'fail', 'ok'), if(x = 1, 'fail', 'ok'), if(x >= 0, 'fail', 'ok')
FROM (SELECT if(1 = 0, toNullable(to_uint8(0)), NULL) AS x);

SELECT to_type_name(x), x, isNull(x), if(x = 0, 'fail', 'ok'), if(x = 1, 'fail', 'ok'), if(x >= 0, 'fail', 'ok')
FROM (SELECT materialize(if(1 = 0, toNullable(to_uint8(0)), NULL)) AS x);

SET join_use_nulls = 1;

SELECT b_num, isNull(b_num), to_type_name(b_num), b_num = 0, if(b_num = 0, 'fail', 'ok')
FROM (SELECT 1 k, to_int8(1) a_num) AS x
LEFT JOIN (SELECT 2 k, to_int8(1) b_num) AS y
USING (k);

-- test case from https://github.com/ClickHouse/ClickHouse/issues/7347
DROP STREAM IF EXISTS test_nullable_float_issue7347;
create stream test_nullable_float_issue7347 (ne uint64,test Nullable(float64)) ENGINE = MergeTree() PRIMARY KEY (ne) ORDER BY (ne);
INSERT INTO test_nullable_float_issue7347 VALUES (1,NULL);

SELECT test, to_type_name(test), IF(test = 0, 1, 0) FROM test_nullable_float_issue7347;

WITH materialize(CAST(NULL, 'Nullable(float64)')) AS test SELECT test, to_type_name(test), IF(test = 0, 1, 0);

DROP STREAM test_nullable_float_issue7347;

-- test case from https://github.com/ClickHouse/ClickHouse/issues/10846

SELECT if(is_finite(toUInt64OrZero(toNullable('123'))), 1, 0);

SELECT if(materialize(is_finite(toUInt64OrZero(toNullable('123')))), 1, 0);
