-- Tags: no-fasttest

SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS json;
create stream json (x enum8('browser' = 1, 'mobile' = 2), y string) ;

INSERT INTO json (y) VALUES ('Hello');

SELECT sleep(3);

SELECT * FROM json ORDER BY y;

INSERT INTO json (y) FORMAT JSONEachRow {"y": "World 1"};
SELECT sleep(3);
SELECT * FROM json ORDER BY y;

INSERT INTO json (x, y) FORMAT JSONEachRow {"y": "World 2"};
SELECT sleep(3);
SELECT * FROM json ORDER BY y;

DROP STREAM json;
