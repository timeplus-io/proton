DROP STREAM IF EXISTS defaults;
CREATE STREAM defaults
(
	n int32
)ENGINE = Memory();

INSERT INTO defaults SELECT * FROM numbers(10);

SELECT * FROM defaults;

TRUNCATE defaults;

SELECT * FROM defaults;

DROP STREAM defaults;
