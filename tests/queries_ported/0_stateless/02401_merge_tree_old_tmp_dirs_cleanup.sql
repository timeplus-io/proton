-- Tags: no-fasttest

DROP STREAM IF EXISTS test_inserts;

CREATE STREAM test_inserts (`key` int, `part` int) ENGINE = MergeTree PARTITION BY part ORDER BY key
SETTINGS temporary_directories_lifetime = 0, merge_tree_clear_old_temporary_directories_interval_seconds = 0;

INSERT INTO test_inserts SELECT sleep(1), number FROM numbers(10)
SETTINGS max_insert_delayed_streams_for_parallel_write = 100, max_insert_block_size = 1, min_insert_block_size_rows = 1;

SELECT count(), sum(part) FROM test_inserts;

DROP STREAM test_inserts;
