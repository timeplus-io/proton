SET enable_optimize_predicate_expression = 1;
set query_mode='table';
set asterisk_include_reserved_columns=false;

DROP TABLE IF EXISTS test1_00843;
DROP TABLE IF EXISTS test2_00843;
DROP TABLE IF EXISTS view_00843;

create stream test1_00843 (a uint8) ;
INSERT INTO test1_00843(a) VALUES (1);

SELECT sleep(3);
CREATE VIEW view_00843 AS SELECT * FROM test1_00843;
SELECT * FROM view_00843;
RENAME TABLE test1_00843 TO test2_00843;
SELECT * FROM view_00843; -- { serverError 60 }
RENAME TABLE test2_00843 TO test1_00843;
SELECT * FROM view_00843;

DROP TABLE test1_00843;
DROP TABLE view_00843;
