SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS nullable_00465;
create stream nullable_00465 (id nullable(uint32), cat string)  ;
INSERT INTO nullable_00465 (cat) VALUES ('test');
SELECT sleep(3);
SELECT * FROM nullable_00465;
DROP STREAM nullable_00465;
