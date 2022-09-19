DROP STREAM IF EXISTS non_ascii;
create stream non_ascii (`привет` string, `мир` string) ;
INSERT INTO non_ascii VALUES ('hello', 'world');
SELECT `привет` FROM non_ascii;
SELECT * FROM non_ascii;
DROP STREAM non_ascii;
