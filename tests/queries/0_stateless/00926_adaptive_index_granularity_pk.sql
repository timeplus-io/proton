-- Tags: no-parallel

SET send_logs_level = 'fatal';
SELECT '----00489----';
DROP STREAM IF EXISTS pk;

create stream pk (d date DEFAULT '2000-01-01', x DateTime, y uint64, z uint64) ENGINE = MergeTree() PARTITION BY d ORDER BY (to_start_of_minute(x), y, z) SETTINGS index_granularity_bytes=19, min_index_granularity_bytes=9, write_final_mark = 0; -- one row granule

INSERT INTO pk (x, y, z) VALUES (1, 11, 1235), (2, 11, 4395), (3, 22, 3545), (4, 22, 6984), (5, 33, 4596), (61, 11, 4563), (62, 11, 4578), (63, 11, 3572), (64, 22, 5786), (65, 22, 5786), (66, 22, 2791), (67, 22, 2791), (121, 33, 2791), (122, 33, 2791), (123, 33, 1235), (124, 44, 4935), (125, 44, 4578), (126, 55, 5786), (127, 55, 2791), (128, 55, 1235);

SET max_block_size = 1;

-- Test inferred limit
SET max_rows_to_read = 5;
SELECT to_uint32(x), y, z FROM pk WHERE x BETWEEN to_datetime(0) AND to_datetime(59);

SET max_rows_to_read = 9;
SELECT to_uint32(x), y, z FROM pk WHERE x BETWEEN to_datetime(120) AND to_datetime(240);

-- Index is coarse, cannot read single row
SET max_rows_to_read = 5;
SELECT to_uint32(x), y, z FROM pk WHERE x = to_datetime(1);

-- Index works on interval 00:01:00 - 00:01:59
SET max_rows_to_read = 4;
SELECT to_uint32(x), y, z FROM pk WHERE (x BETWEEN to_datetime(60) AND to_datetime(119)) AND y = 11;

-- Cannot read less rows as PK is coarser on interval 00:01:00 - 00:02:00
SET max_rows_to_read = 5;
SELECT to_uint32(x), y, z FROM pk WHERE (x BETWEEN to_datetime(60) AND to_datetime(120)) AND y = 11;

DROP STREAM pk;

SET max_block_size = 8192;
SELECT '----00607----';

SET max_rows_to_read = 0;
DROP STREAM IF EXISTS merge_tree;
create stream merge_tree (x uint32) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity_bytes = 4, min_index_granularity_bytes=1, write_final_mark = 0;
INSERT INTO merge_tree VALUES (0), (1);

SET force_primary_key = 1;
SET max_rows_to_read = 1;

SELECT count() FROM merge_tree WHERE x = 0;
SELECT count() FROM merge_tree WHERE to_uint32(x) = 0;
SELECT count() FROM merge_tree WHERE to_uint64(x) = 0;

SELECT count() FROM merge_tree WHERE x IN (0, 0);
SELECT count() FROM merge_tree WHERE to_uint32(x) IN (0, 0);
SELECT count() FROM merge_tree WHERE to_uint64(x) IN (0, 0);

DROP STREAM merge_tree;

SELECT '----00804----';
SET max_rows_to_read = 0;
SET force_primary_key = 0;

DROP STREAM IF EXISTS large_alter_table_00926;
DROP STREAM IF EXISTS store_of_hash_00926;

SET allow_suspicious_codecs = 1;
create stream large_alter_table_00926 (
    somedate date CODEC(ZSTD, ZSTD, ZSTD(12), LZ4HC(12)),
    id uint64 CODEC(LZ4, ZSTD, NONE, LZ4HC),
    data string CODEC(ZSTD(2), LZ4HC, NONE, LZ4, LZ4)
) ENGINE = MergeTree() PARTITION BY somedate ORDER BY id SETTINGS min_index_granularity_bytes=30, write_final_mark = 0, min_bytes_for_wide_part = '10M';

INSERT INTO large_alter_table_00926 SELECT to_date('2019-01-01'), number, to_string(number + rand()) FROM system.numbers LIMIT 300000;

create stream store_of_hash_00926 (hash uint64) ();

INSERT INTO store_of_hash_00926 SELECT sum(cityHash64(*)) FROM large_alter_table_00926;

ALTER STREAM large_alter_table_00926 MODIFY COLUMN data CODEC(NONE, LZ4, LZ4HC, ZSTD);

OPTIMIZE STREAM large_alter_table_00926;

DETACH TABLE large_alter_table_00926;
ATTACH TABLE large_alter_table_00926;

INSERT INTO store_of_hash_00926 SELECT sum(cityHash64(*)) FROM large_alter_table_00926;

SELECT COUNT(hash) FROM store_of_hash_00926;
SELECT COUNT(DISTINCT hash) FROM store_of_hash_00926;

DROP STREAM IF EXISTS large_alter_table_00926;
DROP STREAM IF EXISTS store_of_hash_00926;
