DROP STREAM IF EXISTS index_compact;

CREATE STREAM index_compact(a uint32, b uint32, index i1 b type minmax granularity 1)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1000, index_granularity = 128, merge_max_block_size = 100;

INSERT INTO index_compact SELECT number, to_string(number) FROM numbers(100);
INSERT INTO index_compact SELECT number, to_string(number) FROM numbers(30);

OPTIMIZE STREAM index_compact FINAL;

SELECT count() FROM index_compact WHERE b < 10;

DROP STREAM index_compact;
