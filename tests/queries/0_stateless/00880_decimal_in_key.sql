DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

create stream t1 (str string, dec Decimal64(8)) ENGINE = MergeTree ORDER BY str;
create stream t2 (str string, dec Decimal64(8)) ENGINE = MergeTree ORDER BY dec;

INSERT INTO t1 SELECT to_string(number), to_decimal64(number, 8) FROM system.numbers LIMIT 1000000;
SELECT count() FROM t1;

INSERT INTO t2 SELECT to_string(number), to_decimal64(number, 8) FROM system.numbers LIMIT 1000000;
SELECT count() FROM t2;

DROP STREAM t1;
DROP STREAM t2;
