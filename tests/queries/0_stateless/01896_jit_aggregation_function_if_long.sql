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
    value4 uint64,

    predicate_value uint8
) ;

INSERT INTO test_table_unsigned_values SELECT number % 3, number, number, number, number, if(number % 2 == 0, 1, 0) FROM system.numbers LIMIT 120;
SELECT
    id,
    sum_if(value1, predicate_value),
    sum_if(value2, predicate_value),
    sum_if(value3, predicate_value),
    sum_if(value4, predicate_value)
FROM test_table_unsigned_values GROUP BY id ORDER BY id;
DROP STREAM test_table_unsigned_values;

SELECT 'Test signed integer values';

DROP STREAM IF EXISTS test_table_signed_values;
create stream test_table_signed_values
(
    id uint64,

    value1 int8,
    value2 int16,
    value3 int32,
    value4 int64,

    predicate_value uint8
) ;

INSERT INTO test_table_signed_values SELECT number % 3, number, number, number, number, if(number % 2 == 0, 1, 0) FROM system.numbers LIMIT 120;
SELECT
    id,
    sum_if(value1, predicate_value),
    sum_if(value2, predicate_value),
    sum_if(value3, predicate_value),
    sum_if(value4, predicate_value)
FROM test_table_signed_values GROUP BY id ORDER BY id;
DROP STREAM test_table_signed_values;

SELECT 'Test float values';

DROP STREAM IF EXISTS test_table_float_values;
create stream test_table_float_values
(
    id uint64,

    value1 float32,
    value2 float64,

    predicate_value uint8
) ;

INSERT INTO test_table_float_values SELECT number % 3, number, number, if(number % 2 == 0, 1, 0) FROM system.numbers LIMIT 120;
SELECT
    id,
    sum_if(value1, predicate_value),
    sum_if(value2, predicate_value)
FROM test_table_float_values GROUP BY id ORDER BY id;
DROP STREAM test_table_float_values;

SELECT 'Test nullable unsigned integer values';

DROP STREAM IF EXISTS test_table_nullable_unsigned_values;
create stream test_table_nullable_unsigned_values
(
    id uint64,

    value1 nullable(uint8),
    value2 nullable(uint16),
    value3 nullable(uint32),
    value4 nullable(uint64),

    predicate_value uint8
) ;

INSERT INTO test_table_nullable_unsigned_values SELECT number % 3, number, number, number, number, if(number % 2 == 0, 1, 0)  FROM system.numbers LIMIT 120;
SELECT
    id,
    sum_if(value1, predicate_value),
    sum_if(value2, predicate_value),
    sum_if(value3, predicate_value),
    sum_if(value4, predicate_value)
FROM test_table_nullable_unsigned_values GROUP BY id ORDER BY id;
DROP STREAM test_table_nullable_unsigned_values;

SELECT 'Test nullable signed integer values';

DROP STREAM IF EXISTS test_table_nullable_signed_values;
create stream test_table_nullable_signed_values
(
    id uint64,

    value1 nullable(int8),
    value2 nullable(int16),
    value3 nullable(int32),
    value4 nullable(int64),

    predicate_value uint8
) ;

INSERT INTO test_table_nullable_signed_values SELECT number % 3, number, number, number, number, if(number % 2 == 0, 1, 0) FROM system.numbers LIMIT 120;
SELECT
    id,
    sum_if(value1, predicate_value),
    sum_if(value2, predicate_value),
    sum_if(value3, predicate_value),
    sum_if(value4, predicate_value)
FROM test_table_nullable_signed_values GROUP BY id ORDER BY id;
DROP STREAM test_table_nullable_signed_values;

SELECT 'Test nullable float values';

DROP STREAM IF EXISTS test_table_nullable_float_values;
create stream test_table_nullable_float_values
(
    id uint64,

    value1 nullable(float32),
    value2 nullable(float64),

    predicate_value uint8
) ;

INSERT INTO test_table_nullable_float_values SELECT number % 3, number, number, if(number % 2 == 0, 1, 0) FROM system.numbers LIMIT 120;
SELECT
    id,
    sum_if(value1, predicate_value),
    sum_if(value2, predicate_value)
FROM test_table_nullable_float_values GROUP BY id ORDER BY id;
DROP STREAM test_table_nullable_float_values;

SELECT 'Test null specifics';

DROP STREAM IF EXISTS test_table_null_specifics;
create stream test_table_null_specifics
(
    id uint64,

    value1 nullable(uint64),
    value2 nullable(uint64),
    value3 nullable(uint64),

    predicate_value uint8
) ;

INSERT INTO test_table_null_specifics VALUES (0, 1, 1, NULL, 1);
INSERT INTO test_table_null_specifics VALUES (0, 2, NULL, NULL, 1);
INSERT INTO test_table_null_specifics VALUES (0, 3, 3, NULL, 1);

SELECT
    id,
    sum_if(value1, predicate_value),
    sum_if(value2, predicate_value),
    sum_if(value3, predicate_value)
FROM test_table_null_specifics GROUP BY id ORDER BY id;
DROP STREAM IF EXISTS test_table_null_specifics;

SELECT 'Test null variadic';

DROP STREAM IF EXISTS test_table_null_specifics;
create stream test_table_null_specifics
(
    id uint64,

    value1 nullable(uint64),
    value2 nullable(uint64),
    value3 nullable(uint64),

    predicate_value uint8,
    weight uint64
) ;

INSERT INTO test_table_null_specifics VALUES (0, 1, 1, NULL, 1, 1);
INSERT INTO test_table_null_specifics VALUES (0, 2, NULL, NULL, 1, 2);
INSERT INTO test_table_null_specifics VALUES (0, 3, 3, NULL, 1, 3);

SELECT
    id,
    avg_weighted_if(value1, weight, predicate_value),
    avg_weighted_if(value2, weight, predicate_value),
    avg_weighted_if(value3, weight, predicate_value)
FROM test_table_null_specifics GROUP BY id ORDER BY id;
DROP STREAM IF EXISTS test_table_null_specifics;
