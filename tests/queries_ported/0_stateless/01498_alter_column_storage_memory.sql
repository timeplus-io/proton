DROP STREAM IF EXISTS defaults;
CREATE STREAM defaults
(
	n int32,
	s string
)ENGINE = Memory();

ALTER STREAM defaults ADD COLUMN m int8; -- { serverError 48 }
ALTER STREAM defaults DROP COLUMN n; -- { serverError 48 }

DROP STREAM defaults;
