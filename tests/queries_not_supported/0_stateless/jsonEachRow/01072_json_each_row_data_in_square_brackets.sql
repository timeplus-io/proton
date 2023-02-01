-- Tags: no-fasttest

DROP STREAM IF EXISTS json_square_brackets;
CREATE STREAM json_square_brackets (id uint32, name string) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT jsonEachRow [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];
INSERT INTO json_square_brackets FORMAT jsonEachRow[];
INSERT INTO json_square_brackets FORMAT jsonEachRow [  ]  ;
INSERT INTO json_square_brackets FORMAT jsonEachRow ;

SELECT * FROM json_square_brackets ORDER BY id;
DROP STREAM IF EXISTS json_square_brackets;
