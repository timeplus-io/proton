DROP STREAM IF EXISTS t;

create stream t
(
  number uint64
)
ENGINE = MergeTree
ORDER BY number
SETTINGS index_granularity = 128;

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
INSERT INTO t SELECT number FROM numbers(10000000);

SET max_threads = 1, max_block_size = 12345;
SELECT  array_distinct(arrayPopFront(arrayDifference(group_array(number)))) FROM t;

DROP STREAM t;
