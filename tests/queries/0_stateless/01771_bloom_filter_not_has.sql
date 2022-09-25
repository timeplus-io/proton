DROP STREAM IF EXISTS bloom_filter_null_array;
create stream bloom_filter_null_array (v array(int32), INDEX idx v TYPE bloom_filter GRANULARITY 3) ENGINE = MergeTree() ORDER BY v;
INSERT INTO bloom_filter_null_array SELECT [number] FROM numbers(10000000);
SELECT count() FROM bloom_filter_null_array;
SELECT count() FROM bloom_filter_null_array WHERE has(v, 0);
SELECT count() FROM bloom_filter_null_array WHERE not has(v, 0);
DROP STREAM bloom_filter_null_array;
