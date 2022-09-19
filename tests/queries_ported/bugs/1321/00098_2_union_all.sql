SET query_mode = 'table';
DROP STREAM IF EXISTS data2013;
DROP STREAM IF EXISTS data2014;

create stream data2013 (name string, value uint32);
create stream data2014 (name string, value uint32);

INSERT INTO data2013(name,value) VALUES('Alice', 1000);
INSERT INTO data2013(name,value) VALUES('Bob', 2000);
INSERT INTO data2013(name,value) VALUES('Carol', 5000);

INSERT INTO data2014(name,value) VALUES('Alice', 2000);
INSERT INTO data2014(name,value) VALUES('Bob', 2000);
INSERT INTO data2014(name,value) VALUES('Dennis', 35000);

SELECT sleep(3);

SELECT val FROM
(SELECT value AS val FROM data2013 WHERE name = 'Alice'
UNION   ALL
SELECT value AS val FROM data2014 WHERE name = 'Alice')
ORDER BY val ASC;

DROP STREAM data2013;
DROP STREAM data2014;
