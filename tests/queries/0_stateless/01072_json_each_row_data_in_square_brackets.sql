-- Tags: no-fasttest

DROP STREAM IF EXISTS json_square_brackets;
create stream json_square_brackets (id uint32, name string) ;
INSERT INTO json_square_brackets FORMAT JSONEachRow [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];
INSERT INTO json_square_brackets FORMAT JSONEachRow[];
INSERT INTO json_square_brackets FORMAT JSONEachRow [  ]  ;
INSERT INTO json_square_brackets FORMAT JSONEachRow ;

SELECT * FROM json_square_brackets ORDER BY id;
DROP STREAM IF EXISTS json_square_brackets;
