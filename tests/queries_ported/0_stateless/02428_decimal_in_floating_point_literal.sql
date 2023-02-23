SELECT to_decimal32(1.555,3) IN (1.5551);
SELECT to_decimal32(1.555,3) IN (1.5551,1.555);
SELECT to_decimal32(1.555,3) IN (1.5551,1.555000);
SELECT to_decimal32(1.555,3) IN (1.550,1.5);

SELECT to_decimal64(1.555,3) IN (1.5551);
SELECT to_decimal64(1.555,3) IN (1.5551,1.555);
SELECT to_decimal64(1.555,3) IN (1.5551,1.555000);
SELECT to_decimal64(1.555,3) IN (1.550,1.5);

SELECT to_decimal128(1.555,3) IN (1.5551);
SELECT to_decimal128(1.555,3) IN (1.5551,1.555);
SELECT to_decimal128(1.555,3) IN (1.5551,1.555000);
SELECT to_decimal128(1.555,3) IN (1.550,1.5);

SELECT to_decimal256(1.555,3) IN (1.5551);
SELECT to_decimal256(1.555,3) IN (1.5551,1.555);
SELECT to_decimal256(1.555,3) IN (1.5551,1.555000);
SELECT to_decimal256(1.555,3) IN (1.550,1.5);

DROP STREAM IF EXISTS decimal_in_float_test;

CREATE STREAM decimal_in_float_test ( `a` decimal(18, 0), `b` decimal(36, 2) ) ENGINE = Memory;
INSERT INTO decimal_in_float_test VALUES ('33', '44.44');

SELECT count() == 1 FROM decimal_in_float_test WHERE a IN (33);
SELECT count() == 1 FROM decimal_in_float_test WHERE a IN (33.0);
SELECT count() == 1 FROM decimal_in_float_test WHERE a NOT IN (33.333);
SELECT count() == 1 FROM decimal_in_float_test WHERE b IN (44.44);
SELECT count() == 1 FROM decimal_in_float_test WHERE b NOT IN (44.4,44.444);

DROP STREAM IF EXISTS decimal_in_float_test;
