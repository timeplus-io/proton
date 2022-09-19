-- Tags: no-parallel

DROP STREAM IF EXISTS mt;

create stream mt (x uint64) ENGINE = MergeTree ORDER BY x SETTINGS max_part_loading_threads = 16, parts_to_delay_insert = 100000, parts_to_throw_insert = 100000;

SYSTEM STOP MERGES mt;

SET max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
INSERT INTO mt SELECT * FROM numbers(1000);
SET max_block_size = 65536;

SELECT count(), sum(x) FROM mt;

DETACH TABLE mt;
ATTACH TABLE mt;

SELECT count(), sum(x) FROM mt;

SYSTEM START MERGES mt;
DROP STREAM mt;
