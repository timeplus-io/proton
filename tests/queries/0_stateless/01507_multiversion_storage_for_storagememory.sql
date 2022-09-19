DROP STREAM IF EXISTS defaults;
create stream defaults
(
	n int32
)();

INSERT INTO defaults SELECT * FROM numbers(10);

SELECT * FROM defaults;

TRUNCATE defaults;

SELECT * FROM defaults;

DROP STREAM defaults;
