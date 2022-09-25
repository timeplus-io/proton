DROP STREAM IF EXISTS tuple;

create stream tuple
(
    `j` tuple(a int8, b string)
)
;

SHOW create stream tuple FORMAT TSVRaw;
DESC tuple;
DROP STREAM tuple;

create stream tuple  AS SELECT CAST((1, 'Test'), 'tuple(a int8,  b string)') AS j;

SHOW create stream tuple FORMAT TSVRaw;
DESC tuple;
DROP STREAM tuple;
