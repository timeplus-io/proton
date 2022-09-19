SET query_mode = 'table';
DROP STREAM IF EXISTS insert_fewer_columns;
create stream insert_fewer_columns (a uint8, b uint8);
INSERT INTO insert_fewer_columns (a) VALUES (1), (2);
SELECT sleep(3);
SELECT a,b FROM insert_fewer_columns;

-- Test position arguments in insert.
DROP STREAM IF EXISTS insert_fewer_columns_2;
create stream insert_fewer_columns_2 (b uint8, a uint8);
INSERT INTO insert_fewer_columns_2 SELECT * FROM insert_fewer_columns;
SELECT sleep(3);
SELECT a, b FROM insert_fewer_columns;
SELECT a, b FROM insert_fewer_columns_2;

DROP STREAM IF EXISTS insert_fewer_columns_2;
DROP STREAM insert_fewer_columns;
