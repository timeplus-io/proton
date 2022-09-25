SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS distinct;
create stream distinct (Num uint32, Name string) ;

INSERT INTO distinct (Num, Name) VALUES (1, 'John');
INSERT INTO distinct (Num, Name) VALUES (1, 'John');
INSERT INTO distinct (Num, Name) VALUES (3, 'Mary');
INSERT INTO distinct (Num, Name) VALUES (3, 'Mary');
INSERT INTO distinct (Num, Name) VALUES (3, 'Mary');
INSERT INTO distinct (Num, Name) VALUES (4, 'Mary');
INSERT INTO distinct (Num, Name) VALUES (4, 'Mary');
INSERT INTO distinct (Num, Name) VALUES (5, 'Bill');
INSERT INTO distinct (Num, Name) VALUES (7, 'Bill');
INSERT INTO distinct (Num, Name) VALUES (7, 'Bill');
INSERT INTO distinct (Num, Name) VALUES (7, 'Mary');
INSERT INTO distinct (Num, Name) VALUES (7, 'John');

SELECT sleep(3);

-- string field
SELECT Name FROM (SELECT DISTINCT Name FROM distinct) ORDER BY Name;
-- Num field
SELECT Num FROM (SELECT DISTINCT Num FROM distinct) ORDER BY Num;

DROP STREAM IF EXISTS distinct;
