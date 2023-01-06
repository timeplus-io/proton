SET query_mode='table';
DROP STREAM IF EXISTS where_qualified;
create stream where_qualified(a uint32, b uint8) ;
INSERT INTO where_qualified(a,b) VALUES(1, 1);
INSERT INTO where_qualified(a,b) VALUES(2, 0);
select sleep(3);
SELECT a from where_qualified WHERE where_qualified.b;
DROP STREAM where_qualified;
