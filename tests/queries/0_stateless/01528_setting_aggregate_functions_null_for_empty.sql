DROP STREAM IF EXISTS defaults;

create stream defaults
(
	n int8
)();

SELECT sum(n) FROM defaults;
SELECT sumOrNull(n) FROM defaults;
SELECT count(n) FROM defaults;
SELECT countOrNull(n) FROM defaults;

SET aggregate_functions_null_for_empty=1;

SELECT sum(n) FROM defaults;
SELECT sumOrNull(n) FROM defaults;
SELECT count(n) FROM defaults;
SELECT countOrNull(n) FROM defaults;

INSERT INTO defaults SELECT * FROM numbers(10);

SET aggregate_functions_null_for_empty=0;

SELECT sum(n) FROM defaults;
SELECT sumOrNull(n) FROM defaults;
SELECT count(n) FROM defaults;
SELECT countOrNull(n) FROM defaults;

SET aggregate_functions_null_for_empty=1;

SELECT sum(n) FROM defaults;
SELECT sumOrNull(n) FROM defaults;
SELECT count(n) FROM defaults;
SELECT countOrNull(n) FROM defaults;

DROP STREAM defaults;
