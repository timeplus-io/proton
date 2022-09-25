DROP STREAM IF EXISTS tutorial;
create stream tutorial ( inner_poly  array(tuple(int32, int32)), outer_poly  array(tuple(int32, int32)) ) engine = Log();

SELECT * FROM tutorial;

INSERT INTO tutorial VALUES ([(123, 456), (789, 234)], [(567, 890)]), ([], [(11, 22), (33, 44), (55, 66)]);
SELECT * FROM tutorial;

DROP STREAM tutorial;
