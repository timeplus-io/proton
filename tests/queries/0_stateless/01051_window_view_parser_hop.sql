SET allow_experimental_window_view = 1;

DROP STREAM IF EXISTS mt;

create stream mt(a int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();

SELECT '---WATERMARK---';
DROP STREAM IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv WATERMARK=INTERVAL '1' SECOND AS SELECT count(a), hopStart(wid) AS w_start, hopEnd(wid) AS w_end FROM mt GROUP BY hop(timestamp, INTERVAL '3' SECOND, INTERVAL '5' SECOND) AS wid;

SELECT '---With w_end---';
DROP STREAM IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv AS SELECT count(a), hopStart(wid) AS w_start, hopEnd(wid) AS w_end FROM mt GROUP BY hop(timestamp, INTERVAL '3' SECOND, INTERVAL '5' SECOND) AS wid;

SELECT '---WithOut w_end---';
DROP STREAM IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv AS SELECT count(a), hopStart(wid) AS w_start FROM mt GROUP BY hop(timestamp, INTERVAL '3' SECOND, INTERVAL '5' SECOND) AS wid;

SELECT '---WITH---';
DROP STREAM IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv AS WITH to_datetime('2018-01-01 00:00:00') AS date_time SELECT count(a), hopStart(wid) AS w_start, hopEnd(wid) AS w_end, date_time FROM mt GROUP BY hop(timestamp, INTERVAL '3' SECOND, INTERVAL '5' SECOND) AS wid;

SELECT '---WHERE---';
DROP STREAM IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv AS SELECT count(a), hopStart(wid) AS w_start FROM mt WHERE a != 1 GROUP BY hop(timestamp, INTERVAL '3' SECOND, INTERVAL '5' SECOND) AS wid;

SELECT '---ORDER_BY---';
DROP STREAM IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv AS SELECT count(a), hopStart(wid) AS w_start FROM mt WHERE a != 1 GROUP BY hop(timestamp, INTERVAL '3' SECOND, INTERVAL '5' SECOND) AS wid ORDER BY w_start;

SELECT '---With now---';
DROP STREAM IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv AS SELECT count(a), hopStart(wid) AS w_start, hopEnd(hop(now(), INTERVAL '1' SECOND, INTERVAL '3' SECOND)) as w_end FROM mt GROUP BY hop(now(), INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
