DROP STREAM IF EXISTS pk;
create stream pk (x datetime) ENGINE = MergeTree ORDER BY to_start_of_minute(x) SETTINGS index_granularity = 1;
SELECT * FROM pk WHERE x >= to_datetime(120) AND x <= to_datetime(NULL);
DROP STREAM pk;
