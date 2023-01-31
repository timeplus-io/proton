SET query_mode='table';
SET asterisk_include_reserved_columns=false;
DROP STREAM IF EXISTS temp;
create stream temp
(
    x Decimal(38, 2),
    y Nullable(Decimal(38, 2))
) ENGINE = Memory;

INSERT INTO temp(x,y) VALUES (32, 32), (64, 64), (128, 128);

SELECT sleep(3);
SELECT * FROM temp WHERE x IN (to_decimal128(128, 2));
SELECT * FROM temp WHERE y IN (to_decimal128(128, 2));

SELECT * FROM temp WHERE x IN (toDecimal32(32, 1));
SELECT * FROM temp WHERE x IN (toDecimal32(32, 2));
SELECT * FROM temp WHERE x IN (toDecimal32(32, 3));
SELECT * FROM temp WHERE y IN (toDecimal32(32, 1));
SELECT * FROM temp WHERE y IN (toDecimal32(32, 2));
SELECT * FROM temp WHERE y IN (toDecimal32(32, 3));

SELECT * FROM temp WHERE x IN (toDecimal64(64, 1));
SELECT * FROM temp WHERE x IN (toDecimal64(64, 2));
SELECT * FROM temp WHERE x IN (toDecimal64(64, 3));
SELECT * FROM temp WHERE y IN (toDecimal64(64, 1));
SELECT * FROM temp WHERE y IN (toDecimal64(64, 2));
SELECT * FROM temp WHERE y IN (toDecimal64(64, 3));

SELECT * FROM temp WHERE x IN (toDecimal256(256, 1)); -- { serverError 53 }
SELECT * FROM temp WHERE y IN (toDecimal256(256, 1)); -- { serverError 53 }

DROP TABLE IF EXISTS temp;
