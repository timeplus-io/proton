-- https://github.com/ClickHouse/ClickHouse/issues/50570

DROP STREAM IF EXISTS tnul SYNC;
DROP STREAM IF EXISTS tlc SYNC;

CREATE STREAM tnul (lc nullable(string), k int) ENGINE = MergeTree ORDER BY k; --tuple()
INSERT INTO tnul(lc, k) VALUES (NULL, 1), ('qwe', 2);
SELECT 'pure nullable result:';
SELECT lc FROM tnul WHERE not_in(lc, ('rty', 'uiop'));
DROP STREAM tnul SYNC;


CREATE STREAM tlc (lc low_cardinality(nullable(string)), k int) ENGINE = MergeTree ORDER BY k; --tuple()
INSERT INTO tlc(lc, k) VALUES (NULL, 1), ('qwe', 2);
SELECT 'wrapping in LC:';
SELECT lc FROM tlc WHERE not_in(lc, ('rty', 'uiop'));
DROP STREAM tlc SYNC;