DROP STREAM IF EXISTS defaults;
create stream defaults
(
	n int32,
	s string
)();

INSERT INTO defaults VALUES(1, '1') (2, '2') (3, '3') (4, '4') (5, '5');

SELECT * FROM defaults;

ALTER STREAM defaults UPDATE n = 100 WHERE s = '1';

SELECT * FROM defaults;

SELECT count(*) FROM defaults;

ALTER STREAM defaults DELETE WHERE n = 100;

SELECT * FROM defaults;

SELECT count(*) FROM defaults;

DROP STREAM defaults;
