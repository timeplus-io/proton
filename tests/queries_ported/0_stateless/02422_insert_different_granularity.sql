SELECT '=== ataptive granularity: stream one -; stream two + ===';

DROP STREAM IF EXISTS stream_one;
CREATE STREAM stream_one (id uint64, value uint64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 100;

DROP STREAM IF EXISTS stream_two;
CREATE STREAM stream_two (id uint64, value uint64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, min_bytes_for_wide_part = 100;

INSERT INTO stream_one SELECT int_div(number, 10), number   FROM numbers(100);

ALTER STREAM stream_two REPLACE PARTITION 0 FROM stream_one;

SELECT '=== ataptive granularity: stream one -; stream two - ===';

DROP STREAM IF EXISTS stream_one;

CREATE STREAM stream_one (id uint64, value uint64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 100;

DROP STREAM IF EXISTS stream_two;

CREATE STREAM stream_two (id uint64, value uint64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 100;

INSERT INTO stream_one SELECT int_div(number, 10), number   FROM numbers(100);

ALTER STREAM stream_two REPLACE PARTITION 0 FROM stream_one;

SELECT '=== ataptive granularity: stream one +; stream two + ===';

DROP STREAM IF EXISTS stream_one;
CREATE STREAM stream_one (id uint64, value uint64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, min_bytes_for_wide_part = 100;

DROP STREAM IF EXISTS stream_two;
CREATE STREAM stream_two (id uint64, value uint64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, min_bytes_for_wide_part = 100;

INSERT INTO stream_one SELECT int_div(number, 10), number   FROM numbers(100);

ALTER STREAM stream_two REPLACE PARTITION 0 FROM stream_one;

SELECT '=== ataptive granularity: stream one +; stream two - ===';

DROP STREAM IF EXISTS stream_one;
CREATE STREAM stream_one (id uint64, value uint64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, min_bytes_for_wide_part = 100;

DROP STREAM IF EXISTS stream_two;
CREATE STREAM stream_two (id uint64, value uint64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 100;

INSERT INTO stream_one SELECT int_div(number, 10), number   FROM numbers(100);

ALTER STREAM stream_two REPLACE PARTITION 0 FROM stream_one; -- { serverError 36 }
