DROP STREAM IF EXISTS calendar;
DROP STREAM IF EXISTS events32;

CREATE STREAM calendar ( `year` int64, `month` int64 ) ENGINE = TinyLog;
INSERT INTO calendar VALUES (2000, 1), (2001, 2), (2000, 3);

CREATE STREAM events32 ( `year` int32, `month` int32 ) ENGINE = TinyLog;
INSERT INTO events32 VALUES (2001, 2), (2001, 3);

SELECT * FROM calendar WHERE (year, month) IN ( SELECT (year, month) FROM events32 );

DROP STREAM IF EXISTS calendar;
DROP STREAM IF EXISTS events32;
