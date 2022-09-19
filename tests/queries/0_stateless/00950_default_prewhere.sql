
DROP STREAM IF EXISTS test_generic_events_all;

create stream test_generic_events_all (APIKey uint8, SessionType uint8) ENGINE = MergeTree() PARTITION BY APIKey ORDER BY tuple();
INSERT INTO test_generic_events_all VALUES( 42, 42 );
ALTER STREAM test_generic_events_all ADD COLUMN OperatingSystem uint64 DEFAULT 42;
SELECT OperatingSystem FROM test_generic_events_all PREWHERE APIKey = 42 WHERE SessionType = 42;
SELECT * FROM test_generic_events_all PREWHERE APIKey = 42 WHERE SessionType = 42;

DROP STREAM IF EXISTS test_generic_events_all;

create stream test_generic_events_all (APIKey uint8, SessionType uint8) ENGINE = MergeTree() PARTITION BY APIKey ORDER BY tuple();
INSERT INTO test_generic_events_all VALUES( 42, 42 );
ALTER STREAM test_generic_events_all ADD COLUMN OperatingSystem uint64 DEFAULT SessionType+1;
SELECT * FROM test_generic_events_all WHERE APIKey = 42 AND SessionType = 42;
SELECT OperatingSystem FROM test_generic_events_all WHERE APIKey = 42;
SELECT OperatingSystem FROM test_generic_events_all WHERE APIKey = 42 AND SessionType = 42;
SELECT OperatingSystem FROM test_generic_events_all PREWHERE APIKey = 42 WHERE SessionType = 42;
SELECT * FROM test_generic_events_all PREWHERE APIKey = 42 WHERE SessionType = 42;

DROP STREAM IF EXISTS test_generic_events_all;
