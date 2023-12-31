DROP STREAM IF EXISTS data_01655;
CREATE STREAM data_01655 (key int) Engine=MergeTree() ORDER BY key;
INSERT INTO data_01655 VALUES (1);
SELECT * FROM data_01655 SETTINGS merge_tree_min_rows_for_concurrent_read=0, merge_tree_min_bytes_for_concurrent_read=0;
-- UINT64_MAX
SELECT * FROM data_01655 SETTINGS merge_tree_min_rows_for_concurrent_read=18446744073709551615, merge_tree_min_bytes_for_concurrent_read=18446744073709551615;

DROP STREAM data_01655;
