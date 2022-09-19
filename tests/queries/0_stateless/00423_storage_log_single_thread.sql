-- Tags: no-parallel

DROP STREAM IF EXISTS log;
create stream log (s string)  ;

SELECT * FROM log LIMIT 1;
SELECT * FROM log;

DETACH TABLE log;
ATTACH TABLE log;

SELECT * FROM log;
SELECT * FROM log LIMIT 1;

INSERT INTO log VALUES ('Hello'), ('World');

SELECT * FROM log LIMIT 1;

DETACH TABLE log;
ATTACH TABLE log;

SELECT * FROM log LIMIT 1;
SELECT * FROM log;

DETACH TABLE log;
ATTACH TABLE log;

SELECT * FROM log;
SELECT * FROM log LIMIT 1;

DROP STREAM log;
