-- Tags: no-parallel

DROP STREAM IF EXISTS log;
create stream log (s string)  ;

SELECT * FROM log LIMIT 1;
SELECT * FROM log;

DETACH STREAM log;
ATTACH STREAM log;

SELECT * FROM log;
SELECT * FROM log LIMIT 1;

INSERT INTO log VALUES ('Hello'), ('World');

SELECT * FROM log LIMIT 1;

DETACH STREAM log;
ATTACH STREAM log;

SELECT * FROM log LIMIT 1;
SELECT * FROM log;

DETACH STREAM log;
ATTACH STREAM log;

SELECT * FROM log;
SELECT * FROM log LIMIT 1;

DROP STREAM log;
