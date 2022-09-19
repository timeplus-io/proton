DROP STREAM IF EXISTS empty;
DROP STREAM IF EXISTS data;

create stream empty (value int8) ;
create stream data (value int8) ;

INSERT INTO data SELECT * FROM empty;
SELECT * FROM data;

INSERT INTO data SELECT 1;
SELECT * FROM data;

DROP STREAM empty;
DROP STREAM data;
