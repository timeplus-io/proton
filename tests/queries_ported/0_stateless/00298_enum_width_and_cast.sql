SET query_mode='table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS enum;

create stream enum (x enum8('Hello' = -100, '\\' = 0, '\t\\t' = 111), y uint8) ;
INSERT INTO enum (y) VALUES (0);

SELECT sleep(3);

SELECT * FROM enum ORDER BY x, y FORMAT PrettyCompact;
INSERT INTO enum (x) VALUES ('\\');

SELECT sleep(3);

SELECT * FROM enum ORDER BY x, y FORMAT PrettyCompact;
INSERT INTO enum (x) VALUES ('\t\\t');

SELECT sleep(3);

SELECT * FROM enum ORDER BY x, y FORMAT PrettyCompact;
SELECT x, y, to_int8(x), to_string(x) AS s, CAST(s AS enum8('Hello' = -100, '\\' = 0, '\t\\t' = 111)) AS casted FROM enum ORDER BY x, y FORMAT PrettyCompact;

DROP STREAM enum;
