-- Tags: long

SET compile_aggregate_expressions = 1;
SET min_count_to_compile_aggregate_expression = 0;

SELECT 'Test unsigned integer values';

DROP STREAM IF EXISTS test_table_unsigned_values;
create stream test_table_unsigned_values
(
    id uint64,

    value1 uint8,
    value2 uint16,
    value3 uint32,
    value4 uint64
) ;

INSERT INTO test_table_unsigned_values SELECT number % 3, number, number, number, number FROM system.numbers LIMIT 120;
SELECT id, max(value1), max(value2), max(value3), max(value4) FROM test_table_unsigned_values GROUP BY id ORDER BY id;
DROP STREAM test_table_unsigned_values;

SELECT 'Test signed integer values';

DROP STREAM IF EXISTS test_table_signed_values;
create stream test_table_signed_values
(
    id uint64,

    value1 int8,
    value2 Int16,
    value3 int32,
    value4 int64
) ;

INSERT INTO test_table_signed_values SELECT number % 3, number, number, number, number FROM system.numbers LIMIT 120;
SELECT id, max(value1), max(value2), max(value3), max(value4) FROM test_table_signed_values GROUP BY id ORDER BY id;
DROP STREAM test_table_signed_values;

SELECT 'Test float values';

DROP STREAM IF EXISTS test_table_float_values;
create stream test_table_float_values
(
    id uint64,

    value1 Float32,
    value2 float64
) ;

INSERT INTO test_table_float_values SELECT number % 3, number, number FROM system.numbers LIMIT 120;
SELECT id, min(value1), min(value2) FROM test_table_float_values GROUP BY id ORDER BY id;
DROP STREAM test_table_float_values;

SELECT 'Test nullable unsigned integer values';

DROP STREAM IF EXISTS test_table_nullable_unsigned_values;
create stream test_table_nullable_unsigned_values
(
    id uint64,

    value1 Nullable(uint8),
    value2 Nullable(uint16),
    value3 Nullable(uint32),
    value4 Nullable(uint64)
) ;

INSERT INTO test_table_nullable_unsigned_values SELECT number % 3, number, number, number, number FROM system.numbers LIMIT 120;
SELECT id, max(value1), max(value2), max(value3), max(value4) FROM test_table_nullable_unsigned_values GROUP BY id ORDER BY id;
DROP STREAM test_table_nullable_unsigned_values;

SELECT 'Test nullable signed integer values';

DROP STREAM IF EXISTS test_table_nullable_signed_values;
create stream test_table_nullable_signed_values
(
    id uint64,

    value1 Nullable(int8),
    value2 Nullable(Int16),
    value3 Nullable(int32),
    value4 Nullable(int64)
) ;

INSERT INTO test_table_nullable_signed_values SELECT number % 3, number, number, number, number FROM system.numbers LIMIT 120;
SELECT id, max(value1), max(value2), max(value3), max(value4) FROM test_table_nullable_signed_values GROUP BY id ORDER BY id;
DROP STREAM test_table_nullable_signed_values;

SELECT 'Test nullable float values';

DROP STREAM IF EXISTS test_table_nullable_float_values;
create stream test_table_nullable_float_values
(
    id uint64,

    value1 Nullable(Float32),
    value2 Nullable(float64)
) ;

INSERT INTO test_table_nullable_float_values SELECT number % 3, number, number FROM system.numbers LIMIT 120;
SELECT id, max(value1), max(value2) FROM test_table_nullable_float_values GROUP BY id ORDER BY id;
DROP STREAM test_table_nullable_float_values;

SELECT 'Test null specifics';

DROP STREAM IF EXISTS test_table_null_specifics;
create stream test_table_null_specifics
(
    id uint64,

    value1 Nullable(uint64),
    value2 Nullable(uint64),
    value3 Nullable(uint64)
) ;

INSERT INTO test_table_null_specifics VALUES (0, 1, 1, NULL);
INSERT INTO test_table_null_specifics VALUES (0, 2, NULL, NULL);
INSERT INTO test_table_null_specifics VALUES (0, 3, 3, NULL);

SELECT id, max(value1), max(value2), max(value3) FROM test_table_null_specifics GROUP BY id ORDER BY id;
DROP STREAM IF EXISTS test_table_null_specifics;
