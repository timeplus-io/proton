-- Tags: no-parallel

SET allow_experimental_window_view = 1;
DROP DATABASE IF EXISTS test_01048;
CREATE DATABASE test_01048 ENGINE=Ordinary;

DROP STREAM IF EXISTS test_01048.mt;
DROP STREAM IF EXISTS test_01048.mt_2;

create stream test_01048.mt(a int32, b int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
create stream test_01048.mt_2(a int32, b int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();

SELECT '---TUMBLE---';
SELECT '||---WINDOW COLUMN NAME---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, tumbleEnd(wid) as wend FROM test_01048.mt GROUP BY tumble(timestamp, INTERVAL 1 SECOND) as wid;
SHOW create stream test_01048.`.inner.wv`;

SELECT '||---WINDOW COLUMN ALIAS---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, tumble(timestamp, INTERVAL '1' SECOND) AS wid FROM test_01048.mt GROUP BY wid;
SHOW create stream test_01048.`.inner.wv`;

SELECT '||---IDENTIFIER---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY b, tumble(timestamp, INTERVAL '1' SECOND) AS wid;
SHOW create stream test_01048.`.inner.wv`;

DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY tumble(timestamp, INTERVAL '1' SECOND) AS wid, b;
SHOW create stream test_01048.`.inner.wv`;

SELECT '||---FUNCTION---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY plus(a, b) as _type, tumble(timestamp, INTERVAL '1' SECOND) AS wid;
SHOW create stream test_01048.`.inner.wv`;

DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY tumble(timestamp, INTERVAL '1' SECOND) AS wid, plus(a, b);
SHOW create stream test_01048.`.inner.wv`;

SELECT '||---TimeZone---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, tumble(timestamp, INTERVAL '1' SECOND, 'Asia/Shanghai') AS wid FROM test_01048.mt GROUP BY wid;
SHOW create stream test_01048.`.inner.wv`;

SELECT '||---DATA COLUMN ALIAS---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, b as id FROM test_01048.mt GROUP BY id, tumble(timestamp, INTERVAL '1' SECOND);
SHOW create stream test_01048.`.inner.wv`;

SELECT '||---JOIN---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(test_01048.mt.a), count(test_01048.mt_2.b), wid FROM test_01048.mt JOIN test_01048.mt_2 ON test_01048.mt.timestamp = test_01048.mt_2.timestamp GROUP BY tumble(test_01048.mt.timestamp, INTERVAL '1' SECOND) AS wid;
SHOW create stream test_01048.`.inner.wv`;


SELECT '---HOP---';
SELECT '||---WINDOW COLUMN NAME---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, hopEnd(wid) as wend FROM test_01048.mt GROUP BY hop(timestamp, INTERVAL 1 SECOND, INTERVAL 3 SECOND) as wid;
SHOW create stream test_01048.`.inner.wv`;

SELECT '||---WINDOW COLUMN ALIAS---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid FROM test_01048.mt GROUP BY wid;
SHOW create stream test_01048.`.inner.wv`;

SELECT '||---IDENTIFIER---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY b, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW create stream test_01048.`.inner.wv`;

DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid, b;
SHOW create stream test_01048.`.inner.wv`;

SELECT '||---FUNCTION---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY plus(a, b) as _type, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW create stream test_01048.`.inner.wv`;

DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid, plus(a, b);
SHOW create stream test_01048.`.inner.wv`;

SELECT '||---TimeZone---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, hopEnd(wid) as wend FROM test_01048.mt GROUP BY hop(timestamp, INTERVAL 1 SECOND, INTERVAL 3 SECOND, 'Asia/Shanghai') as wid;
SHOW create stream test_01048.`.inner.wv`;

SELECT '||---DATA COLUMN ALIAS---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, b as id FROM test_01048.mt GROUP BY id, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND);
SHOW create stream test_01048.`.inner.wv`;

SELECT '||---JOIN---';
DROP STREAM IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(test_01048.mt.a), count(test_01048.mt_2.b), wid FROM test_01048.mt JOIN test_01048.mt_2 ON test_01048.mt.timestamp = test_01048.mt_2.timestamp GROUP BY hop(test_01048.mt.timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW create stream test_01048.`.inner.wv`;

DROP STREAM test_01048.wv;
DROP STREAM test_01048.mt;
DROP STREAM test_01048.mt_2;
