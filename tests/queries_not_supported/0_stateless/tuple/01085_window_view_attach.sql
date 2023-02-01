-- Tags: no-parallel

SET allow_experimental_window_view = 1;

DROP DATABASE IF EXISTS test_01085;
CREATE DATABASE test_01085 ENGINE=Ordinary;

DROP STREAM IF EXISTS test_01085.mt;
DROP STREAM IF EXISTS test_01085.wv;

CREATE STREAM test_01085.mt(a int32, market int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW test_01085.wv ENGINE Memory WATERMARK=ASCENDING AS SELECT count(a) AS count, market, tumbleEnd(wid) AS w_end FROM test_01085.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid, market;

SHOW streams FROM test_01085;

DROP STREAM test_01085.wv NO DELAY;
SHOW streams FROM test_01085;

CREATE WINDOW VIEW test_01085.wv ENGINE Memory WATERMARK=ASCENDING AS SELECT count(a) AS count, market, tumbleEnd(wid) AS w_end FROM test_01085.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid, market;

DETACH STREAM test_01085.wv;
SHOW streams FROM test_01085;

ATTACH STREAM test_01085.wv;
SHOW streams FROM test_01085;

DROP STREAM test_01085.wv NO DELAY;
SHOW streams FROM test_01085;
