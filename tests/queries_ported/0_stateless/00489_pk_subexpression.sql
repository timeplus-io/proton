SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS pk;

create stream pk (d date DEFAULT '2000-01-01', x datetime, y uint64, z uint64) ENGINE = MergeTree(d, (to_start_of_minute(x), y, z), 1);

INSERT INTO pk (x, y, z) VALUES (1, 11, 1235), (2, 11, 4395), (3, 22, 3545), (4, 22, 6984), (5, 33, 4596), (61, 11, 4563), (62, 11, 4578), (63, 11, 3572), (64, 22, 5786), (65, 22, 5786), (66, 22, 2791), (67, 22, 2791), (121, 33, 2791), (122, 33, 2791), (123, 33, 1235), (124, 44, 4935), (125, 44, 4578), (126, 55, 5786), (127, 55, 2791), (128, 55, 1235);
SELECT sleep(3);
SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
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
