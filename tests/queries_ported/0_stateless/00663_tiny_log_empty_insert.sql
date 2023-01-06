SET query_mode='table';
SET asterisk_include_reserved_columns=false;
DROP STREAM IF EXISTS empty;
DROP STREAM IF EXISTS data;

create stream empty (value int8) ;
create stream data (value int8) ;

INSERT INTO data(value) SELECT * FROM empty;
select sleep(3);
SELECT * FROM data;

INSERT INTO data(value) SELECT 1;
select sleep(3);
SELECT * FROM data;

DROP STREAM empty;
DROP STREAM data;
