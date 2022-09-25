DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
DROP STREAM IF EXISTS t3;
DROP STREAM IF EXISTS t4;

create stream t1 AS remote('127.0.0.1', system.one);
SELECT count() FROM t1;

create stream t2 AS remote('127.0.0.1', system.numbers);
SELECT * FROM t2 LIMIT 18;

create stream t3 AS remote('127.0.0.1', numbers(100));
SELECT * FROM t3 where number > 17 and number < 25;

create stream t4 AS numbers(100);
SELECT count() FROM t4 where number > 74;

DROP STREAM t1;
DROP STREAM t2;
DROP STREAM t3;
DROP STREAM t4;
