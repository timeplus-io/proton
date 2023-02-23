DROP STREAM IF EXISTS dtest;

SELECT count() == 0 FROM (SELECT '33.3' :: decimal(9, 1) AS a WHERE a IN ('33.33' :: decimal(9, 2)));

CREATE STREAM dtest ( `a` decimal(18, 0), `b` decimal(18, 1), `c` decimal(36, 0) ) ENGINE = Memory;
INSERT INTO dtest VALUES ('33', '44.4', '35');

SELECT count() == 0 FROM dtest WHERE a IN to_decimal32('33.3000', 4);
SELECT count() == 0 FROM dtest WHERE a IN to_decimal64('33.3000', 4);
SELECT count() == 0 FROM dtest WHERE a IN to_decimal128('33.3000', 4);
SELECT count() == 0 FROM dtest WHERE a IN to_decimal256('33.3000', 4); -- { serverError 53 }

SELECT count() == 0 FROM dtest WHERE b IN to_decimal32('44.4000', 0);
SELECT count() == 0 FROM dtest WHERE b IN to_decimal64('44.4000', 0);
SELECT count() == 0 FROM dtest WHERE b IN to_decimal128('44.4000', 0);
SELECT count() == 0 FROM dtest WHERE b IN to_decimal256('44.4000', 0); -- { serverError 53 }

SELECT count() == 1 FROM dtest WHERE b IN to_decimal32('44.4000', 4);
SELECT count() == 1 FROM dtest WHERE b IN to_decimal64('44.4000', 4);
SELECT count() == 1 FROM dtest WHERE b IN to_decimal128('44.4000', 4);
SELECT count() == 1 FROM dtest WHERE b IN to_decimal256('44.4000', 4); -- { serverError 53 }

DROP STREAM IF EXISTS dtest;
