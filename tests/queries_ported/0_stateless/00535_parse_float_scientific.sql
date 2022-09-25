SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;


DROP STREAM IF EXISTS float;
create stream float (x float64)  ;

INSERT INTO float(x) VALUES (1e7);
SELECT sleep(3);
SELECT * FROM float;

DROP STREAM float;
