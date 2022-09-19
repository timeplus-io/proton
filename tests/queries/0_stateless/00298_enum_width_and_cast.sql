DROP STREAM IF EXISTS enum;

create stream enum (x Enum8('Hello' = -100, '\\' = 0, '\t\\t' = 111), y uint8) ;
INSERT INTO enum (y) VALUES (0);
SELECT * FROM enum ORDER BY x, y FORMAT PrettyCompact;
INSERT INTO enum (x) VALUES ('\\');
SELECT * FROM enum ORDER BY x, y FORMAT PrettyCompact;
INSERT INTO enum (x) VALUES ('\t\\t');
SELECT * FROM enum ORDER BY x, y FORMAT PrettyCompact;
SELECT x, y, to_int8(x), to_string(x) AS s, CAST(s AS Enum8('Hello' = -100, '\\' = 0, '\t\\t' = 111)) AS casted FROM enum ORDER BY x, y FORMAT PrettyCompact;

DROP STREAM enum;
