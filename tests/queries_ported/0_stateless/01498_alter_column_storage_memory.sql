DROP STREAM IF EXISTS defaults;
CREATE STREAM defaults
(
	n int32,
	s string
)ENGINE = Memory();

ALTER STREAM defaults ADD COLUMN m int8;
ALTER STREAM defaults DROP COLUMN n;

DESC STREAM defaults;

DROP STREAM defaults;
