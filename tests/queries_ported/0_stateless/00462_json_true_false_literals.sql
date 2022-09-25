-- Tags: no-fasttest

SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;


DROP STREAM IF EXISTS json;
create stream json (x uint8, title string) ;
INSERT INTO json FORMAT JSONEachRow {"x": true, "title": "true"}, {"x": false, "title": "false"}, {"x": 0, "title": "0"}, {"x": 1, "title": "1"};
SELECT sleep(3);
SELECT * FROM json ORDER BY title;
DROP STREAM IF EXISTS json;
