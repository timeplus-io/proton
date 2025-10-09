-- Tags: long

-- This test was split in two due to long runtimes in sanitizers.
-- The other part is 00284_external_aggregation.

SET group_by_two_level_threshold_bytes = 50000000;
SET max_memory_usage = 0;
SET group_by_two_level_threshold = 100000;
SET max_bytes_before_external_group_by = '1Mi';
-- SET max_bytes_ratio_before_external_group_by = 0;

-- method: key_string & key_string_two_level
CREATE STREAM t_00284_str(s string) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
SELECT sleep(1) FORMAT Null;
INSERT INTO t_00284_str(s) SELECT to_string(number) FROM numbers_mt(1e6);
INSERT INTO t_00284_str(s) SELECT to_string(number) FROM numbers_mt(1e6);
SELECT sleep(3) FORMAT Null;
SELECT s, count() FROM table(t_00284_str) GROUP BY s ORDER BY s LIMIT 10 OFFSET 42;

-- method: low_cardinality_key_string & low_cardinality_key_string_two_level
CREATE STREAM t_00284_lc_str(s low_cardinality(string)) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
SELECT sleep(1) FORMAT Null;
INSERT INTO t_00284_lc_str(s) SELECT to_string(number) FROM numbers_mt(1e6);
INSERT INTO t_00284_lc_str(s) SELECT to_string(number) FROM numbers_mt(1e6);
SELECT sleep(3) FORMAT Null;
SELECT s, count() FROM table(t_00284_lc_str) GROUP BY s ORDER BY s LIMIT 10 OFFSET 42;

