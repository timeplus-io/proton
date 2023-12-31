DROP STREAM IF EXISTS temp;
CREATE STREAM temp
(
    x decimal(38, 2),
    y nullable(decimal(38, 2))
) ENGINE = Memory;

INSERT INTO temp VALUES (32, 32), (64, 64), (128, 128);

SELECT * FROM temp WHERE x IN (to_decimal128(128, 1));
SELECT * FROM temp WHERE x IN (to_decimal128(128, 2));
SELECT * FROM temp WHERE x IN (to_decimal128(128, 3));
SELECT * FROM temp WHERE y IN (to_decimal128(128, 1));
SELECT * FROM temp WHERE y IN (to_decimal128(128, 2));
SELECT * FROM temp WHERE y IN (to_decimal128(128, 3));

SELECT * FROM temp WHERE x IN (to_decimal32(32, 1));
SELECT * FROM temp WHERE x IN (to_decimal32(32, 2));
SELECT * FROM temp WHERE x IN (to_decimal32(32, 3));
SELECT * FROM temp WHERE y IN (to_decimal32(32, 1));
SELECT * FROM temp WHERE y IN (to_decimal32(32, 2));
SELECT * FROM temp WHERE y IN (to_decimal32(32, 3));

SELECT * FROM temp WHERE x IN (to_decimal64(64, 1));
SELECT * FROM temp WHERE x IN (to_decimal64(64, 2));
SELECT * FROM temp WHERE x IN (to_decimal64(64, 3));
SELECT * FROM temp WHERE y IN (to_decimal64(64, 1));
SELECT * FROM temp WHERE y IN (to_decimal64(64, 2));
SELECT * FROM temp WHERE y IN (to_decimal64(64, 3));

SELECT * FROM temp WHERE x IN (to_decimal256(256, 1)); -- { serverError 53 }
SELECT * FROM temp WHERE y IN (to_decimal256(256, 1)); -- { serverError 53 }

DROP STREAM IF EXISTS temp;
