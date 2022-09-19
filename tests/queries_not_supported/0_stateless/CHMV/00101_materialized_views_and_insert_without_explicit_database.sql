-- Tags: no-parallel
SET query_mode='table';
CREATE DATABASE IF NOT EXISTS test_00101_0;

USE test_00101_0;

DROP STREAM IF EXISTS test_table;
DROP STREAM IF EXISTS test_view;
DROP STREAM IF EXISTS test_view_filtered;

create stream test_table (EventDate date, CounterID uint32,  UserID uint64,  EventTime datetime('Europe/Moscow'), UTCEventTime datetime('UTC')) ENGINE = MergeTree(EventDate, CounterID, 8192);
CREATE MATERIALIZED VIEW test_view (Rows uint64,  MaxHitTime datetime('Europe/Moscow'))  AS SELECT count() AS Rows, max(UTCEventTime) AS MaxHitTime FROM test_table;
CREATE MATERIALIZED VIEW test_view_filtered (EventDate date, CounterID uint32)  POPULATE AS SELECT CounterID, EventDate FROM test_table WHERE EventDate < '2013-01-01';

INSERT INTO test_table (EventDate, UTCEventTime) VALUES ('2014-01-02', '2014-01-02 03:04:06');
SELECT sleep(3);
SELECT * FROM test_table;
SELECT * FROM test_view;
SELECT * FROM test_view_filtered;

DROP STREAM test_table;
DROP STREAM test_view;
DROP STREAM test_view_filtered;

-- Check only sophisticated constructors and desctructors:

CREATE DATABASE IF NOT EXISTS test_00101_1;

USE test_00101_1;

DROP STREAM IF EXISTS tmp;
DROP STREAM IF EXISTS tmp_mv;
DROP STREAM IF EXISTS tmp_mv2;
DROP STREAM IF EXISTS tmp_mv3;
DROP STREAM IF EXISTS tmp_mv4;
DROP STREAM IF EXISTS `.inner.tmp_mv`;
DROP STREAM IF EXISTS `.inner.tmp_mv2`;
DROP STREAM IF EXISTS `.inner.tmp_mv3`;
DROP STREAM IF EXISTS `.inner.tmp_mv4`;

create stream tmp (date date, name string) ;
CREATE MATERIALIZED VIEW tmp_mv ENGINE = AggregatingMergeTree(date, (date, name), 8192) AS SELECT date, name, countState() AS cc FROM tmp GROUP BY date, name;
create stream tmp_mv2 AS tmp_mv;
create stream tmp_mv3 AS tmp_mv ;
CREATE MATERIALIZED VIEW tmp_mv4 ENGINE = AggregatingMergeTree(date, date, 8192) POPULATE AS SELECT DISTINCT * FROM tmp_mv;

DROP STREAM tmp_mv;
DROP STREAM tmp_mv2;
DROP STREAM tmp_mv3;
DROP STREAM tmp_mv4;

EXISTS TABLE `.inner.tmp_mv`;
EXISTS TABLE `.inner.tmp_mv2`;
EXISTS TABLE `.inner.tmp_mv3`;
EXISTS TABLE `.inner.tmp_mv4`;

DROP STREAM tmp;

DROP DATABASE test_00101_0;
DROP DATABASE test_00101_1;
