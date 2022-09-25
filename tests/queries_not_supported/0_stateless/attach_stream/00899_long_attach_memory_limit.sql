-- Tags: long, no-debug, no-parallel, no-fasttest

DROP STREAM IF EXISTS index_memory;
create stream index_memory (x uint64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO index_memory SELECT * FROM system.numbers LIMIT 5000000;
SELECT count() FROM index_memory;
DETACH STREAM index_memory;
SET max_memory_usage = 39000000;
ATTACH STREAM index_memory;
SELECT count() FROM index_memory;
DROP STREAM index_memory;
