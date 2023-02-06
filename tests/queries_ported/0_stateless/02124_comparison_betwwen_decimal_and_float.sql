select CAST(1.0, 'decimal(15,2)') > CAST(1, 'float64');
select CAST(1.0, 'decimal(15,2)') = CAST(1, 'float64');
select CAST(1.0, 'decimal(15,2)') < CAST(1, 'float64');
select CAST(1.0, 'decimal(15,2)') != CAST(1, 'float64');
select CAST(1.0, 'decimal(15,2)') > CAST(-1, 'float64');
select CAST(1.0, 'decimal(15,2)') = CAST(-1, 'float64');
select CAST(1.0, 'decimal(15,2)') < CAST(-1, 'float64');
select CAST(1.0, 'decimal(15,2)') != CAST(-1, 'float64');
select CAST(1.0, 'decimal(15,2)') > CAST(1, 'float32');
select CAST(1.0, 'decimal(15,2)') = CAST(1, 'float32');
select CAST(1.0, 'decimal(15,2)') < CAST(1, 'float32');
select CAST(1.0, 'decimal(15,2)') != CAST(1, 'float32');
select CAST(1.0, 'decimal(15,2)') > CAST(-1, 'float32');
select CAST(1.0, 'decimal(15,2)') = CAST(-1, 'float32');
select CAST(1.0, 'decimal(15,2)') < CAST(-1, 'float32');
select CAST(1.0, 'decimal(15,2)') != CAST(-1, 'float32');

SELECT to_decimal32('11.00', 2) > 1.;

SELECT 0.1000000000000000055511151231257827021181583404541015625::decimal256(70) = 0.1;

DROP STREAM IF EXISTS t;

CREATE STREAM t
(
	d1 decimal32(5),
	d2 decimal64(10),
	d3 decimal128(30),
	d4 decimal256(50),
	f1 float32,
	f2 float32
)ENGINE = Memory;

INSERT INTO t values (-1.5, -1.5, -1.5, -1.5, 1.5, 1.5);
INSERT INTO t values (1.5, 1.5, 1.5, 1.5, -1.5, -1.5);

SELECT d1 > f1 FROM t ORDER BY f1;
SELECT d2 > f1 FROM t ORDER BY f1;
SELECT d3 > f1 FROM t ORDER BY f1;
SELECT d4 > f1 FROM t ORDER BY f1;

SELECT d1 > f2 FROM t ORDER BY f2;
SELECT d2 > f2 FROM t ORDER BY f2;
SELECT d3 > f2 FROM t ORDER BY f2;
SELECT d4 > f2 FROM t ORDER BY f2;

DROP STREAM t;
