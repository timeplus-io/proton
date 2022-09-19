-- Tags: no-fasttest

DROP STREAM IF EXISTS json_square_brackets;
create stream json_square_brackets (field string) ;
INSERT INTO json_square_brackets FORMAT JSONAsString [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];
INSERT INTO json_square_brackets FORMAT JSONAsString[];
INSERT INTO json_square_brackets FORMAT JSONAsString [  ]  ;
INSERT INTO json_square_brackets FORMAT JSONEachRow ;

SELECT * FROM json_square_brackets;
DROP STREAM IF EXISTS json_square_brackets;
