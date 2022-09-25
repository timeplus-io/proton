SET query_mode='table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS log;

create stream log (x uint8) ENGINE = StripeLog;

SELECT * FROM log ORDER BY x;
INSERT INTO log(x) VALUES (0);

SELECT sleep(3);

SELECT * FROM log ORDER BY x;
INSERT INTO log(x) VALUES (1);

SELECT sleep(3);

SELECT * FROM log ORDER BY x;
INSERT INTO log(x) VALUES (2);

SELECT sleep(3);

SELECT * FROM log ORDER BY x;

DROP STREAM log;

create stream log (x uint8) ;

SELECT * FROM log ORDER BY x;
INSERT INTO log(x) VALUES (0);

SELECT sleep(3);

SELECT * FROM log ORDER BY x;
INSERT INTO log(x) VALUES (1);

SELECT sleep(3);

SELECT * FROM log ORDER BY x;
INSERT INTO log(x) VALUES (2);

SELECT sleep(3);

SELECT * FROM log ORDER BY x;

DROP STREAM log;

create stream log (x uint8)  ;

SELECT * FROM log ORDER BY x;
INSERT INTO log(x) VALUES (0);

SELECT sleep(3);

SELECT * FROM log ORDER BY x;
INSERT INTO log(x) VALUES (1);

SELECT sleep(3);

SELECT * FROM log ORDER BY x;
INSERT INTO log(x) VALUES (2);

SELECT sleep(3);

SELECT * FROM log ORDER BY x;

DROP STREAM log;
