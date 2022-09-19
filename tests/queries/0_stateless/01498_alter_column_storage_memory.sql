DROP STREAM IF EXISTS defaults;
create stream defaults
(
	n int32,
	s string
)();

ALTER STREAM defaults ADD COLUMN m int8; -- { serverError 48 }
ALTER STREAM defaults DROP COLUMN n; -- { serverError 48 }

DROP STREAM defaults;
