CREATE STREAM stream_02184 (x uint8); --{serverError 119}
SET default_table_engine = 'Log';
CREATE STREAM stream_02184 (x uint8);
show create_02184;
DROP STREAM stream_02184;

SET default_table_engine = 'MergeTree';
CREATE STREAM stream_02184 (x uint8); --{serverError 42}
CREATE STREAM stream_02184 (x uint8, PRIMARY KEY (x));
show create_02184;
DROP STREAM stream_02184;

CREATE STREAM test_optimize_exception (date Date) PARTITION BY to_YYYYMM(date) ORDER BY date;
show create test_optimize_exception;
DROP STREAM test_optimize_exception;
CREATE STREAM stream_02184 (x uint8) PARTITION BY x; --{serverError 36}
CREATE STREAM stream_02184 (x uint8) ORDER BY x;
show create_02184;
DROP STREAM stream_02184;

CREATE STREAM stream_02184 (x uint8) PRIMARY KEY x;
show create_02184;
DROP STREAM stream_02184;
SET default_table_engine = 'Memory';
CREATE STREAM numbers1 AS SELECT number FROM numbers(10);
show create numbers1;
SELECT avg(number) FROM numbers1;
DROP STREAM numbers1;

SET default_table_engine = 'MergeTree';
CREATE STREAM numbers2 ORDER BY intHash32(number) SAMPLE BY intHash32(number) AS SELECT number FROM numbers(10);
SELECT sum(number) FROM numbers2;
show create numbers2;
DROP STREAM numbers2;

CREATE STREAM numbers3 ENGINE = Log AS SELECT number FROM numbers(10);
SELECT sum(number) FROM numbers3;
show create numbers3;
DROP STREAM numbers3;

CREATE STREAM test_table (EventDate Date, CounterID uint32,  UserID uint64,  EventTime DateTime('America/Los_Angeles'), UTCEventTime DateTime('UTC')) PARTITION BY EventDate PRIMARY KEY CounterID;
SET default_table_engine = 'Memory';
CREATE MATERIALIZED VIEW test_view (Rows uint64,  MaxHitTime DateTime('America/Los_Angeles')) AS SELECT count() AS Rows, max(UTCEventTime) AS MaxHitTime FROM test_table;
CREATE MATERIALIZED VIEW test_view_filtered (EventDate Date, CounterID uint32) POPULATE AS SELECT CounterID, EventDate FROM test_table WHERE EventDate < '2013-01-01';
show create test_view_filtered;
INSERT INTO test_table (EventDate, UTCEventTime) VALUES ('2014-01-02', '2014-01-02 03:04:06');

SELECT * FROM test_table;
SELECT * FROM test_view;
SELECT * FROM test_view_filtered;

DROP STREAM test_view;
DROP STREAM test_view_filtered;

SET default_table_engine = 'MergeTree';
CREATE MATERIALIZED VIEW test_view ORDER BY Rows AS SELECT count() AS Rows, max(UTCEventTime) AS MaxHitTime FROM test_table;
SET default_table_engine = 'Memory';
CREATE STREAM t1 AS test_view;
CREATE STREAM t2 ENGINE=Memory AS test_view;
show create t1;
show create t2;
DROP STREAM test_view;
DROP STREAM test_table;
DROP STREAM t1;
DROP STREAM t2;


CREATE DATABASE test_02184 ORDER BY kek; -- {serverError 80}
CREATE DATABASE test_02184 SETTINGS x=1; -- {serverError 115}
CREATE STREAM stream_02184 (x uint8, y int, PRIMARY KEY (x)) ENGINE=MergeTree PRIMARY KEY y; -- {clientError 36}
SET default_table_engine = 'MergeTree';
CREATE STREAM stream_02184 (x uint8, y int, PRIMARY KEY (x)) PRIMARY KEY y; -- {clientError 36}

CREATE STREAM mt (a uint64, b nullable(string), PRIMARY KEY (a, coalesce(b, 'test')), INDEX b_index b TYPE set(123) GRANULARITY 1);
show create mt;
SET default_table_engine = 'Log';
CREATE STREAM mt2 AS mt;
show create mt2;
DROP STREAM mt;

SET default_table_engine = 'Log';
CREATE TEMPORARY STREAM tmp (n int);
SHOW CREATE TEMPORARY STREAM tmp;
CREATE TEMPORARY STREAM tmp1 (n int) ENGINE=Memory;
CREATE TEMPORARY STREAM tmp2 (n int) ENGINE=Log; -- {serverError 80}
CREATE TEMPORARY STREAM tmp2 (n int) ORDER BY n; -- {serverError 80}
CREATE TEMPORARY STREAM tmp2 (n int, PRIMARY KEY (n)); -- {serverError 80}

CREATE STREAM log (n int);
SHOW CREATE log;
SET default_table_engine = 'MergeTree';
CREATE STREAM log1 AS log;
SHOW CREATE log1;
CREATE STREAM mem AS log1 ENGINE=Memory;
SHOW CREATE mem;
DROP STREAM log;
DROP STREAM log1;
DROP STREAM mem;

SET default_table_engine = 'None';
CREATE STREAM mem AS SELECT 1 as n; --{serverError 119}
SET default_table_engine = 'Memory';
CREATE STREAM mem ORDER BY n AS SELECT 1 as n; -- {serverError 36}
SET default_table_engine = 'MergeTree';
CREATE STREAM mt ORDER BY n AS SELECT 1 as n;
CREATE STREAM mem ENGINE=Memory AS SELECT 1 as n;
show create mt;
show create mem;
DROP STREAM mt;
DROP STREAM mem;

CREATE STREAM val AS values('n int', 1, 2);
CREATE STREAM val2 AS val;
CREATE STREAM log ENGINE=Log AS val;
show create val;
show create val2;
show create log;
DROP STREAM val;
DROP STREAM val2;
DROP STREAM log;

DROP STREAM IF EXISTS kek;
DROP STREAM IF EXISTS lol;
SET default_table_engine = 'Memory';
CREATE STREAM kek (n int) SETTINGS log_queries=1;
CREATE STREAM lol (n int) ENGINE=MergeTree ORDER BY n SETTINGS min_bytes_for_wide_part=123 SETTINGS log_queries=1;
show create kek;
show create lol;
DROP STREAM kek;
DROP STREAM lol;
