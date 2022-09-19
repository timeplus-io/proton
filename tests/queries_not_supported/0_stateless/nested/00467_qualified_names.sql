SELECT dummy FROM system.one;
SELECT * FROM system.one;
SELECT `one`.dummy FROM system.one;
SELECT one.* FROM system.one;
SELECT system.`one`.dummy FROM system.one;
SELECT `system`.`one`.* FROM system.one;

SELECT `t`.dummy FROM system.one AS t;
SELECT t.* FROM system.one AS t;
SELECT t.dummy FROM system.one t;
SELECT t.* FROM system.one t;

SELECT one.dummy FROM system.one one;
SELECT one.* FROM system.one one;

USE system;

SELECT `dummy` FROM `one`;
SELECT * FROM one;
SELECT one.dummy FROM one;
SELECT one.* FROM one;
SELECT system.one.dummy FROM one;
SELECT system.one.* FROM one;
SELECT system.one.dummy FROM `one` AS `t`;
SELECT system.one.* FROM one AS `t`;

DROP STREAM IF EXISTS nested;
create stream nested (nest nested(a uint8, b string)) ;
INSERT INTO nested VALUES ([1, 2], ['hello', 'world']);
SELECT nest.a, nest.b, nested.`nest`.`a`, nested.nest.b, t.nest.a, t.nest.b, t.* FROM nested AS t;
DROP STREAM nested;

SELECT number FROM numbers(2);
SELECT t.number FROM numbers(2) t;
SELECT x FROM (SELECT 1 AS x);
SELECT t.x FROM (SELECT 1 AS x) t;
