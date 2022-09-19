DROP STREAM IF EXISTS size_hint;
create stream size_hint (s array(string)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;

SET max_block_size = 1000;
SET max_memory_usage = 1000000000;
INSERT INTO size_hint SELECT array_map(x -> 'Hello', range(1000)) FROM numbers(10000);

SET max_memory_usage = 100000000, max_threads = 2;
SELECT count(), sum(length(s)) FROM size_hint;

DROP STREAM size_hint;
