DROP STREAM IF EXISTS t_create_as_tuple;

create stream t_create_as_tuple ENGINE = MergeTree()
ORDER BY number AS
SELECT number, [('string',number)] AS array FROM numbers(3);

SELECT * FROM t_create_as_tuple ORDER BY number;

DETACH STREAM t_create_as_tuple;
ATTACH STREAM t_create_as_tuple;

SELECT * FROM t_create_as_tuple ORDER BY number;

DROP STREAM t_create_as_tuple;
