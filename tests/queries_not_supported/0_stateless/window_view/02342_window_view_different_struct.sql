SET allow_experimental_window_view = 1;

DROP STREAM IF EXISTS data_02342;
DROP STREAM IF EXISTS window_view_02342;

-- ALTER
CREATE STREAM data_02342 (a uint8) ENGINE=MergeTree ORDER BY a;
CREATE WINDOW VIEW window_view_02342 ENGINE=Memory AS SELECT count(a), tumbleStart(wid) AS w_start, tumbleEnd(tumble(now(), INTERVAL '3' SECOND)) AS w_end FROM data_02342 GROUP BY tumble(now(), INTERVAL '3' SECOND) AS wid;
INSERT INTO data_02342 VALUES (42);
ALTER STREAM data_02342 ADD COLUMN s string;
INSERT INTO data_02342 VALUES (42, 'data_02342');
DROP STREAM data_02342;
DROP STREAM window_view_02342;

-- DROP/CREATE
CREATE STREAM data_02342 (a uint8) ENGINE=MergeTree ORDER BY a;
CREATE WINDOW VIEW window_view_02342 ENGINE=Memory AS SELECT count(a), tumbleStart(wid) AS w_start, tumbleEnd(tumble(now(), INTERVAL '3' SECOND)) AS w_end FROM data_02342 GROUP BY tumble(now(), INTERVAL '3' SECOND) AS wid;
INSERT INTO data_02342 VALUES (42);
DROP STREAM data_02342;
CREATE STREAM data_02342 (a uint8, s string) ENGINE=MergeTree ORDER BY a;
INSERT INTO data_02342 VALUES (42, 'data_02342');
DROP STREAM data_02342;
DROP STREAM window_view_02342;
