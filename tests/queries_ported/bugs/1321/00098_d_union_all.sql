SET query_mode = 'table';
DROP STREAM IF EXISTS data2013;
DROP STREAM IF EXISTS data2015;

create stream data2013 (name string, value uint32);
create stream data2015 (data_name string, data_value uint32);

INSERT INTO data2013(name,value) VALUES('Alice', 1000);
INSERT INTO data2013(name,value) VALUES('Bob', 2000);
INSERT INTO data2013(name,value) VALUES('Carol', 5000);

INSERT INTO data2015(data_name, data_value) VALUES('Foo', 42);
INSERT INTO data2015(data_name, data_value) VALUES('Bar', 1);

SELECT sleep(3);

SELECT name FROM (SELECT name FROM data2013 UNION ALL SELECT data_name FROM data2015) ORDER BY name ASC;

DROP STREAM data2013;
DROP STREAM data2015;
