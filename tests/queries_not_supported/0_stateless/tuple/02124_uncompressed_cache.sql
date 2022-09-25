DROP STREAM IF EXISTS t_uncompressed_cache;

create stream t_uncompressed_cache(id uint32, n uint32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
min_compress_block_size = 12, max_compress_block_size = 12,
index_granularity = 4;

INSERT INTO t_uncompressed_cache SELECT number, number FROM numbers(200);

SET max_threads = 1;

SELECT sum(n), count() FROM t_uncompressed_cache PREWHERE id = 0 OR id = 5 OR id = 100 SETTINGS use_uncompressed_cache = 0;
SELECT sum(n), count() FROM t_uncompressed_cache PREWHERE id = 0 OR id = 5 OR id = 100 SETTINGS use_uncompressed_cache = 1;

DROP STREAM t_uncompressed_cache;
