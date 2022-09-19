DROP STREAM IF EXISTS log;

create stream log (x uint8) ENGINE = StripeLog () SETTINGS disk = 'disk_memory';

SELECT * FROM log ORDER BY x;
INSERT INTO log VALUES (0);
SELECT * FROM log ORDER BY x;
INSERT INTO log VALUES (1);
SELECT * FROM log ORDER BY x;
INSERT INTO log VALUES (2);
SELECT * FROM log ORDER BY x;

TRUNCATE TABLE log;
DROP STREAM log;

create stream log (x uint8)  () SETTINGS disk = 'disk_memory';

SELECT * FROM log ORDER BY x;
INSERT INTO log VALUES (0);
SELECT * FROM log ORDER BY x;
INSERT INTO log VALUES (1);
SELECT * FROM log ORDER BY x;
INSERT INTO log VALUES (2);
SELECT * FROM log ORDER BY x;

TRUNCATE TABLE log;
DROP STREAM log;

create stream log (x uint8)   () SETTINGS disk = 'disk_memory';

SELECT * FROM log ORDER BY x;
INSERT INTO log VALUES (0);
SELECT * FROM log ORDER BY x;
INSERT INTO log VALUES (1);
SELECT * FROM log ORDER BY x;
INSERT INTO log VALUES (2);
SELECT * FROM log ORDER BY x;

TRUNCATE TABLE log;
DROP STREAM log;
