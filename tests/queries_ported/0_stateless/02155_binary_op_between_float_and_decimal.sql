SELECT 1.5::decimal32(5) + 1.5;
SELECT 1.5::decimal32(5) - 1.5;
SELECT 1.5::decimal32(5) * 1.5;
SELECT 1.5::decimal32(5) / 1.5;

SELECT 1.5 + 1.5::decimal32(5);
SELECT 1.5 - 1.5::decimal32(5);
SELECT 1.5 * 1.5::decimal32(5);
SELECT 1.5 / 1.5::decimal32(5);

SELECT 1.0::decimal32(5) / 0.0;

SELECT least(1.5, 1.0::decimal32(5));
SELECT greatest(1.5, 1.0::decimal32(5));

DROP STREAM IF EXISTS t;
CREATE STREAM t(d1 decimal32(5), d2 decimal64(10), d3 decimal128(20), d4 decimal256(40), f1 float32, f2 float64) ENGINE=Memory; 

INSERT INTO t values (-4.5, 4.5, -45.5, 45.5, 2.5, -3.5);
INSERT INTO t values (4.5, -4.5, 45.5, -45.5, -3.5, 2.5);
INSERT INTO t values (2.5, -2.5, 25.5, -25.5, -2.5, 3.5);
INSERT INTO t values (-2.5, 2.5, -25.5, 25.5, 3.5, -2.5);

SELECT '';
SELECT 'plus';
SELECT d1, f1, d1 + f1 FROM t ORDER BY f1;
SELECT d2, f1, d2 + f1 FROM t ORDER BY f1;
SELECT d3, f1, d3 + f1 FROM t ORDER BY f1;
SELECT d4, f1, d4 + f1 FROM t ORDER BY f1;

SELECT d1, f2, d1 + f2 FROM t ORDER BY f2;
SELECT d2, f2, d2 + f2 FROM t ORDER BY f2;
SELECT d3, f2, d3 + f2 FROM t ORDER BY f2;
SELECT d4, f2, d4 + f2 FROM t ORDER BY f2;

SELECT '';
SELECT 'minus';
SELECT d1, f1, d1 - f1 FROM t ORDER BY f1;
SELECT d2, f1, d2 - f1 FROM t ORDER BY f1;
SELECT d3, f1, d3 - f1 FROM t ORDER BY f1;
SELECT d4, f1, d4 - f1 FROM t ORDER BY f1;

SELECT d1, f2, d1 - f2 FROM t ORDER BY f2;
SELECT d2, f2, d2 - f2 FROM t ORDER BY f2;
SELECT d3, f2, d3 - f2 FROM t ORDER BY f2;
SELECT d4, f2, d4 - f2 FROM t ORDER BY f2;

SELECT '';
SELECT 'multiply';
SELECT d1, f1, d1 * f1 FROM t ORDER BY f1;
SELECT d2, f1, d2 * f1 FROM t ORDER BY f1;
SELECT d3, f1, d3 * f1 FROM t ORDER BY f1;
SELECT d4, f1, d4 * f1 FROM t ORDER BY f1;

SELECT d1, f2, d1 * f2 FROM t ORDER BY f2;
SELECT d2, f2, d2 * f2 FROM t ORDER BY f2;
SELECT d3, f2, d3 * f2 FROM t ORDER BY f2;
SELECT d4, f2, d4 * f2 FROM t ORDER BY f2;

SELECT '';
SELECT 'division';
SELECT d1, f1, d1 / f1 FROM t ORDER BY f1;
SELECT d2, f1, d2 / f1 FROM t ORDER BY f1;
SELECT d3, f1, d3 / f1 FROM t ORDER BY f1;
SELECT d4, f1, d4 / f1 FROM t ORDER BY f1;

SELECT d1, f2, d1 / f2 FROM t ORDER BY f2;
SELECT d2, f2, d2 / f2 FROM t ORDER BY f2;
SELECT d3, f2, d3 / f2 FROM t ORDER BY f2;
SELECT d4, f2, d4 / f2 FROM t ORDER BY f2;

SELECT '';
SELECT 'least';
SELECT d1, f1, least(d1, f1) FROM t ORDER BY f1;
SELECT d2, f1, least(d2, f1) FROM t ORDER BY f1;
SELECT d3, f1, least(d3, f1) FROM t ORDER BY f1;
SELECT d4, f1, least(d4, f1) FROM t ORDER BY f1;

SELECT d1, f2, least(d1, f2) FROM t ORDER BY f2;
SELECT d2, f2, least(d2, f2) FROM t ORDER BY f2;
SELECT d3, f2, least(d3, f2) FROM t ORDER BY f2;
SELECT d4, f2, least(d4, f2) FROM t ORDER BY f2;

SELECT '';
SELECT 'greatest';
SELECT d1, f1, greatest(d1, f1) FROM t ORDER BY f1;
SELECT d2, f1, greatest(d2, f1) FROM t ORDER BY f1;
SELECT d3, f1, greatest(d3, f1) FROM t ORDER BY f1;
SELECT d4, f1, greatest(d4, f1) FROM t ORDER BY f1;

SELECT d1, f2, greatest(d1, f2) FROM t ORDER BY f2;
SELECT d2, f2, greatest(d2, f2) FROM t ORDER BY f2;
SELECT d3, f2, greatest(d3, f2) FROM t ORDER BY f2;
SELECT d4, f2, greatest(d4, f2) FROM t ORDER BY f2;

DROP STREAM t;
