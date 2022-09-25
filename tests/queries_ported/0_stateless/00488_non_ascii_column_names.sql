SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS non_ascii;
create stream non_ascii (`привет` string, `мир` string) ;
INSERT INTO non_ascii(`привет`, `мир`) VALUES ('hello', 'world');
SELECT sleep(3);
SELECT `привет` FROM non_ascii;
SELECT * FROM non_ascii;
DROP STREAM non_ascii;
