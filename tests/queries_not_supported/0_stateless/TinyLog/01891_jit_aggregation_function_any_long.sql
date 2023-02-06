-- Tags: long

SET compile_aggregate_expressions = 1;
SET min_count_to_compile_aggregate_expression = 0;

SELECT 'Test unsigned integer values';

DROP STREAM IF EXISTS test_table_unsigned_values;
CREATE STREAM test_table_unsigned_values
(
    id uint64,

    value1 uint8,
    value2 uint16,
    value3 uint32,
    value4 uint64
) ENGINE=TinyLog;

INSERT INTO test_table_unsigned_values SELECT number % 3, number, number, number, number FROM system.numbers LIMIT 120;
SELECT id, any(value1), any(value2), any(value3), any(value4) FROM test_table_unsigned_values GROUP BY id ORDER BY id;
DROP STREAM test_table_unsigned_values;

SELECT 'Test signed integer values';

DROP STREAM IF EXISTS test_table_signed_values;
CREATE STREAM test_table_signed_values
(
    id uint64,

    value1 int8,
    value2 int16,
    value3 int32,
    value4 int64
) ENGINE=TinyLog;

INSERT INTO test_table_signed_values SELECT number % 3, number, number, number, number FROM system.numbers LIMIT 120;
SELECT id, any(value1), any(value2), any(value3), any(value4) FROM test_table_signed_values GROUP BY id ORDER BY id;
DROP STREAM test_table_signed_values;

SELECT 'Test float values';

DROP STREAM IF EXISTS test_table_float_values;
CREATE STREAM test_table_float_values
(
    id uint64,

    value1 float32,
    value2 float64
) ENGINE=TinyLog;

INSERT INTO test_table_float_values SELECT number % 3, number, number FROM system.numbers LIMIT 120;
SELECT id, any(value1), any(value2) FROM test_table_float_values GROUP BY id ORDER BY id;
DROP STREAM test_table_float_values;

SELECT 'Test nullable unsigned integer values';

DROP STREAM IF EXISTS test_table_nullable_unsigned_values;
CREATE STREAM test_table_nullable_unsigned_values
(
    id uint64,

    value1 nullable(uint8),
    value2 nullable(uint16),
    value3 nullable(uint32),
    value4 nullable(uint64)
) ENGINE=TinyLog;

INSERT INTO test_table_nullable_unsigned_values SELECT number % 3, number, number, number, number FROM system.numbers LIMIT 120;
SELECT id, any(value1), any(value2), any(value3), any(value4) FROM test_table_nullable_unsigned_values GROUP BY id ORDER BY id;
DROP STREAM test_table_nullable_unsigned_values;

SELECT 'Test nullable signed integer values';

DROP STREAM IF EXISTS test_table_nullable_signed_values;
CREATE STREAM test_table_nullable_signed_values
(
    id uint64,

    value1 nullable(int8),
    value2 nullable(int16),
    value3 nullable(int32),
    value4 nullable(int64)
) ENGINE=TinyLog;

INSERT INTO test_table_nullable_signed_values SELECT number % 3, number, number, number, number FROM system.numbers LIMIT 120;
SELECT id, any(value1), any(value2), any(value3), any(value4) FROM test_table_nullable_signed_values GROUP BY id ORDER BY id;
DROP STREAM test_table_nullable_signed_values;

SELECT 'Test nullable float values';

DROP STREAM IF EXISTS test_table_nullable_float_values;
CREATE STREAM test_table_nullable_float_values
(
    id uint64,

    value1 nullable(float32),
    value2 nullable(float64)
) ENGINE=TinyLog;

INSERT INTO test_table_nullable_float_values SELECT number % 3, number, number FROM system.numbers LIMIT 120;
SELECT id, any(value1), any(value2) FROM test_table_nullable_float_values GROUP BY id ORDER BY id;
DROP STREAM test_table_nullable_float_values;

SELECT 'Test null specifics';

DROP STREAM IF EXISTS test_table_null_specifics;
CREATE STREAM test_table_null_specifics
(
    id uint64,

    value1 nullable(uint64),
    value2 nullable(uint64),
    value3 nullable(uint64)
) ENGINE=TinyLog;

INSERT INTO test_table_null_specifics VALUES (0, 1, 1, NULL);
INSERT INTO test_table_null_specifics VALUES (0, 2, NULL, NULL);
INSERT INTO test_table_null_specifics VALUES (0, 3, 3, NULL);

SELECT id, any(value1), any(value2), any(value3) FROM test_table_null_specifics GROUP BY id ORDER BY id;
DROP STREAM IF EXISTS test_table_null_specifics;
