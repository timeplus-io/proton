DROP STREAM IF EXISTS merge_tree;
create stream merge_tree (x uint32) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
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
