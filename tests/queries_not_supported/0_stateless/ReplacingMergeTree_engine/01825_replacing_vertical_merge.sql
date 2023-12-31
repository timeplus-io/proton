SET optimize_on_insert = 0;

DROP STREAM IF EXISTS replacing_table;

create stream replacing_table (a uint32, b uint32, c uint32)
ENGINE = ReplacingMergeTree ORDER BY a
SETTINGS vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 16,
    min_bytes_for_wide_part = 0,
    merge_max_block_size = 16;

SYSTEM STOP MERGES replacing_table;

INSERT INTO replacing_table SELECT number, number, number from numbers(16);
INSERT INTO replacing_table SELECT 100, number, number from numbers(16);

SELECT sum(a), count() FROM replacing_table;

SYSTEM START MERGES replacing_table;

OPTIMIZE STREAM replacing_table FINAL;

SELECT sum(a), count() FROM replacing_table;

DROP STREAM IF EXISTS replacing_table;

create stream replacing_table
(
    key uint64,
    value uint64
)
ENGINE = ReplacingMergeTree
ORDER BY key
SETTINGS
    vertical_merge_algorithm_min_rows_to_activate=0,
    vertical_merge_algorithm_min_columns_to_activate=0,
    min_bytes_for_wide_part = 0;

INSERT INTO replacing_table SELECT if(number == 8192, 8191, number), 1 FROM numbers(8193);

SELECT sum(key), count() from replacing_table;

OPTIMIZE STREAM replacing_table FINAL;

SELECT sum(key), count() from replacing_table;

DROP STREAM IF EXISTS replacing_table;
