SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS columns;
create stream columns (a uint8, b uint8, c uint8) ;
INSERT INTO columns(a,b,c) VALUES (1, 2, 3);
SELECT sleep(3);
SET max_columns_to_read = 1;

SELECT a FROM (SELECT * FROM columns);
SELECT a FROM (SELECT * FROM (SELECT * FROM columns));
SELECT a FROM (SELECT * FROM columns UNION ALL SELECT * FROM columns);

DROP STREAM columns;
