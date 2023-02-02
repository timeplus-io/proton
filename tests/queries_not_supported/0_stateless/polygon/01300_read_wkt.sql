SELECT read_wkt_point('POINT(0 0)');
SELECT read_wkt_polygon('POLYGON((1 0,10 0,10 10,0 10,1 0))');
SELECT read_wkt_polygon('POLYGON((0 0,10 0,10 10,0 10,0 0),(4 4,5 4,5 5,4 5,4 4))');
SELECT read_wkt_multi_polygon('MULTIPOLYGON(((2 0,10 0,10 10,0 10,2 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))');

DROP STREAM IF EXISTS geo;
CREATE STREAM geo (s string, id int) engine=Memory();
INSERT INTO geo VALUES ('POINT(0 0)', 1);
INSERT INTO geo VALUES ('POINT(1 0)', 2);
INSERT INTO geo VALUES ('POINT(2 0)', 3);
SELECT read_wkt_point(s) FROM geo ORDER BY id;

DROP STREAM IF EXISTS geo;
CREATE STREAM geo (s string, id int) engine=Memory();
INSERT INTO geo VALUES ('POLYGON((1 0,10 0,10 10,0 10,1 0))', 1);
INSERT INTO geo VALUES ('POLYGON((0 0,10 0,10 10,0 10,0 0))', 2);
INSERT INTO geo VALUES ('POLYGON((2 0,10 0,10 10,0 10,2 0))', 3);
INSERT INTO geo VALUES ('POLYGON((0 0,10 0,10 10,0 10,0 0),(4 4,5 4,5 5,4 5,4 4))', 4);
INSERT INTO geo VALUES ('POLYGON((2 0,10 0,10 10,0 10,2 0),(4 4,5 4,5 5,4 5,4 4))', 5);
INSERT INTO geo VALUES ('POLYGON((1 0,10 0,10 10,0 10,1 0),(4 4,5 4,5 5,4 5,4 4))', 6);
SELECT read_wkt_polygon(s) FROM geo ORDER BY id;

DROP STREAM IF EXISTS geo;
CREATE STREAM geo (s string, id int) engine=Memory();
INSERT INTO geo VALUES ('MULTIPOLYGON(((1 0,10 0,10 10,0 10,1 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))', 1);
INSERT INTO geo VALUES ('MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))', 2);
INSERT INTO geo VALUES ('MULTIPOLYGON(((2 0,10 0,10 10,0 10,2 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))', 3);
SELECT read_wkt_multi_polygon(s) FROM geo ORDER BY id;

DROP STREAM geo;
