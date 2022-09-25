DROP STREAM IF EXISTS tuple;
create stream tuple (t tuple(date, uint32, uint64)) ;
INSERT INTO tuple VALUES ((concat('2000', '-01-01'), /* Hello */ 12+3, 45+6));

SET input_format_values_interpret_expressions = 0;
INSERT INTO tuple VALUES (('2000-01-01', 123, 456));

SELECT * FROM tuple ORDER BY t;
DROP STREAM tuple;
