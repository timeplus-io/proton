-- Tags: no-parallel

SET allow_experimental_window_view = 1;
DROP DATABASE IF EXISTS test_01047;
CREATE DATABASE test_01047 ENGINE=Ordinary;

DROP STREAM IF EXISTS test_01047.mt;
DROP STREAM IF EXISTS test_01047.mt_2;

create stream test_01047.mt(a int32, b int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
create stream test_01047.mt_2(a int32, b int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();

SELECT '---TUMBLE---';
SELECT '||---WINDOW COLUMN NAME---';
DROP STREAM IF EXISTS test_01047.wv;
DROP STREAM IF EXISTS test_01047.`.inner.wv`;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY tumble(timestamp, INTERVAL '1' SECOND) AS SELECT count(a), tumbleEnd(wid) AS count FROM test_01047.mt GROUP BY tumble(timestamp, INTERVAL '1' SECOND) as wid;
SHOW create stream test_01047.`.inner.wv`;

SELECT '||---WINDOW COLUMN ALIAS---';
DROP STREAM IF EXISTS test_01047.wv;
DROP STREAM IF EXISTS test_01047.`.inner.wv`;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY wid AS SELECT count(a) AS count, tumble(timestamp, INTERVAL '1' SECOND) AS wid FROM test_01047.mt GROUP BY wid;
SHOW create stream test_01047.`.inner.wv`;

SELECT '||---DATA COLUMN ALIAS---';
DROP STREAM IF EXISTS test_01047.wv;
DROP STREAM IF EXISTS test_01047.`.inner.wv`;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY id AS SELECT count(a) AS count, b as id FROM test_01047.mt GROUP BY id, tumble(timestamp, INTERVAL '1' SECOND);
SHOW create stream test_01047.`.inner.wv`;

SELECT '||---IDENTIFIER---';
DROP STREAM IF EXISTS test_01047.wv;
DROP STREAM IF EXISTS test_01047.`.inner.wv`;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY (tumble(timestamp, INTERVAL '1' SECOND), b) PRIMARY KEY tumble(timestamp, INTERVAL '1' SECOND) AS SELECT count(a) AS count FROM test_01047.mt GROUP BY b, tumble(timestamp, INTERVAL '1' SECOND) AS wid;
SHOW create stream test_01047.`.inner.wv`;

SELECT '||---FUNCTION---';
DROP STREAM IF EXISTS test_01047.wv;
DROP STREAM IF EXISTS test_01047.`.inner.wv`;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY (tumble(timestamp, INTERVAL '1' SECOND), plus(a, b)) PRIMARY KEY tumble(timestamp, INTERVAL '1' SECOND) AS SELECT count(a) AS count FROM test_01047.mt GROUP BY plus(a, b) as _type, tumble(timestamp, INTERVAL '1' SECOND) AS wid;
SHOW create stream test_01047.`.inner.wv`;

SELECT '||---PARTITION---';
DROP STREAM IF EXISTS test_01047.wv;
DROP STREAM IF EXISTS test_01047.`.inner.wv`;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY wid PARTITION BY wid AS SELECT count(a) AS count, tumble(now(), INTERVAL '1' SECOND) AS wid FROM test_01047.mt GROUP BY wid;
SHOW create stream test_01047.`.inner.wv`;

SELECT '||---JOIN---';
DROP STREAM IF EXISTS test_01047.wv;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY tumble(test_01047.mt.timestamp, INTERVAL '1' SECOND) AS SELECT count(test_01047.mt.a), count(test_01047.mt_2.b), wid FROM test_01047.mt JOIN test_01047.mt_2 ON test_01047.mt.timestamp = test_01047.mt_2.timestamp GROUP BY tumble(test_01047.mt.timestamp, INTERVAL '1' SECOND) AS wid;
SHOW create stream test_01047.`.inner.wv`;

DROP STREAM IF EXISTS test_01047.wv;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY wid AS SELECT count(test_01047.mt.a), count(test_01047.mt_2.b), wid FROM test_01047.mt JOIN test_01047.mt_2 ON test_01047.mt.timestamp = test_01047.mt_2.timestamp GROUP BY tumble(test_01047.mt.timestamp, INTERVAL '1' SECOND) AS wid;
SHOW create stream test_01047.`.inner.wv`;


SELECT '---HOP---';
SELECT '||---WINDOW COLUMN NAME---';
DROP STREAM IF EXISTS test_01047.wv;
DROP STREAM IF EXISTS test_01047.`.inner.wv`;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS SELECT count(a) AS count, hopEnd(wid) FROM test_01047.mt GROUP BY hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) as wid;
SHOW create stream test_01047.`.inner.wv`;

SELECT '||---WINDOW COLUMN ALIAS---';
DROP STREAM IF EXISTS test_01047.wv;
DROP STREAM IF EXISTS test_01047.`.inner.wv`;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY wid AS SELECT count(a) AS count, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid FROM test_01047.mt GROUP BY wid;
SHOW create stream test_01047.`.inner.wv`;

SELECT '||---DATA COLUMN ALIAS---';
DROP STREAM IF EXISTS test_01047.wv;
DROP STREAM IF EXISTS test_01047.`.inner.wv`;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY id AS SELECT count(a) AS count, b as id FROM test_01047.mt GROUP BY id, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND);
SHOW create stream test_01047.`.inner.wv`;

SELECT '||---IDENTIFIER---';
DROP STREAM IF EXISTS test_01047.wv;
DROP STREAM IF EXISTS test_01047.`.inner.wv`;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY (hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND), b) PRIMARY KEY hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS SELECT count(a) AS count FROM test_01047.mt GROUP BY b, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW create stream test_01047.`.inner.wv`;

SELECT '||---FUNCTION---';
DROP STREAM IF EXISTS test_01047.wv;
DROP STREAM IF EXISTS test_01047.`.inner.wv`;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY (hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND), plus(a, b)) PRIMARY KEY hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS SELECT count(a) AS count FROM test_01047.mt GROUP BY plus(a, b) as _type, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW create stream test_01047.`.inner.wv`;

SELECT '||---PARTITION---';
DROP STREAM IF EXISTS test_01047.wv;
DROP STREAM IF EXISTS test_01047.`.inner.wv`;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY wid PARTITION BY wid AS SELECT count(a) AS count, hopEnd(wid) FROM test_01047.mt GROUP BY hop(now(), INTERVAL '1' SECOND, INTERVAL '3' SECOND) as wid;
SHOW create stream test_01047.`.inner.wv`;

SELECT '||---JOIN---';
DROP STREAM IF EXISTS test_01047.wv;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY hop(test_01047.mt.timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS SELECT count(test_01047.mt.a), count(test_01047.mt_2.b), wid FROM test_01047.mt JOIN test_01047.mt_2 ON test_01047.mt.timestamp = test_01047.mt_2.timestamp GROUP BY hop(test_01047.mt.timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW create stream test_01047.`.inner.wv`;

DROP STREAM IF EXISTS test_01047.wv;
CREATE WINDOW VIEW test_01047.wv ENGINE AggregatingMergeTree ORDER BY wid AS SELECT count(test_01047.mt.a), count(test_01047.mt_2.b), wid FROM test_01047.mt JOIN test_01047.mt_2 ON test_01047.mt.timestamp = test_01047.mt_2.timestamp GROUP BY hop(test_01047.mt.timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW create stream test_01047.`.inner.wv`;

DROP STREAM test_01047.wv;
DROP STREAM test_01047.mt;
DROP STREAM test_01047.mt_2;
