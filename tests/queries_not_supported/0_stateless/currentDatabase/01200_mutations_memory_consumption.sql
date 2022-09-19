-- Tags: no-debug, no-parallel, long

DROP STREAM IF EXISTS table_with_single_pk;

create stream table_with_single_pk
(
  key uint8,
  value string
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_compress_block_size=65536, max_compress_block_size=65536;

INSERT INTO table_with_single_pk SELECT number, to_string(number % 10) FROM numbers(10000000);

ALTER STREAM table_with_single_pk DELETE WHERE key % 77 = 0 SETTINGS mutations_sync = 1;

SYSTEM FLUSH LOGS;

-- Memory usage for all mutations must be almost constant and less than
-- read_bytes
SELECT
   array_distinct(group_array(if (read_bytes >= peak_memory_usage, [1], [read_bytes, peak_memory_usage])))
FROM
    system.part_log
WHERE event_type = 'MutatePart' AND table = 'table_with_single_pk' AND database = currentDatabase();

DROP STREAM IF EXISTS table_with_single_pk;

DROP STREAM IF EXISTS table_with_multi_pk;

create stream table_with_multi_pk
(
  key1 uint8,
  key2 uint32,
  key3 DateTime64(6, 'UTC'),
  value string
)
ENGINE = MergeTree
ORDER BY (key1, key2, key3)
SETTINGS min_compress_block_size=65536, max_compress_block_size=65536;

INSERT INTO table_with_multi_pk SELECT number % 32, number, to_datetime('2019-10-01 00:00:00'), to_string(number % 10) FROM numbers(10000000);

ALTER STREAM table_with_multi_pk DELETE WHERE key1 % 77 = 0 SETTINGS mutations_sync = 1;

SYSTEM FLUSH LOGS;

-- Memory usage for all mutations must be almost constant and less than
-- read_bytes
SELECT
   array_distinct(group_array(if (read_bytes >= peak_memory_usage, [1], [read_bytes, peak_memory_usage])))
  FROM
      system.part_log
 WHERE event_type = 'MutatePart' AND table = 'table_with_multi_pk' AND database = currentDatabase();

DROP STREAM IF EXISTS table_with_multi_pk;


DROP STREAM IF EXISTS table_with_function_pk;


create stream table_with_function_pk
  (
    key1 uint8,
    key2 uint32,
    key3 DateTime64(6, 'UTC'),
    value string
  )
ENGINE = MergeTree
ORDER BY (cast(value as uint64), key2)
SETTINGS min_compress_block_size=65536, max_compress_block_size=65536;

INSERT INTO table_with_function_pk SELECT number % 32, number, to_datetime('2019-10-01 00:00:00'), to_string(number % 10) FROM numbers(10000000);

ALTER STREAM table_with_function_pk DELETE WHERE key1 % 77 = 0 SETTINGS mutations_sync = 1;

SYSTEM FLUSH LOGS;

-- Memory usage for all mutations must be almost constant and less than
-- read_bytes
SELECT
   array_distinct(group_array(if (read_bytes >= peak_memory_usage, [1], [read_bytes, peak_memory_usage])))
  FROM
      system.part_log
 WHERE event_type = 'MutatePart' AND table = 'table_with_function_pk' AND database = currentDatabase();

DROP STREAM IF EXISTS table_with_function_pk;

DROP STREAM IF EXISTS table_without_pk;

create stream table_without_pk
(
  key1 uint8,
  key2 uint32,
  key3 DateTime64(6, 'UTC'),
  value string
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_compress_block_size=65536, max_compress_block_size=65536;

INSERT INTO table_without_pk SELECT number % 32, number, to_datetime('2019-10-01 00:00:00'), to_string(number % 10) FROM numbers(10000000);

ALTER STREAM table_without_pk DELETE WHERE key1 % 77 = 0 SETTINGS mutations_sync = 1;

SYSTEM FLUSH LOGS;

-- Memory usage for all mutations must be almost constant and less than
-- read_bytes
SELECT
   array_distinct(group_array(if (read_bytes >= peak_memory_usage, [1], [read_bytes, peak_memory_usage])))
  FROM
      system.part_log
 WHERE event_type = 'MutatePart' AND table = 'table_without_pk' AND database = currentDatabase();

DROP STREAM IF EXISTS table_without_pk;
