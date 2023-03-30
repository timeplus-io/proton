
DROP STREAM IF EXISTS bad_skip_idx;

CREATE STREAM bad_skip_idx
(
  id uint64,
  value string
) ENGINE MergeTree()
ORDER BY id SETTINGS index_granularity_bytes = 64, min_index_granularity_bytes = 10, vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0; -- actually vertical merge is not required condition for this bug, but it's more easy to reproduce (becuse we don't recalc granularities)

-- 7 rows per granule
INSERT INTO bad_skip_idx SELECT number, concat('x', to_string(number)) FROM numbers(1000);

-- 3 rows per granule
INSERT INTO bad_skip_idx SELECT number, concat('xxxxxxxxxx', to_string(number)) FROM numbers(1000,1000);

SELECT count(*) from bad_skip_idx WHERE value = 'xxxxxxxxxx1015'; -- check no exception

INSERT INTO bad_skip_idx SELECT number, concat('x', to_string(number)) FROM numbers(1000);

ALTER STREAM bad_skip_idx ADD INDEX idx value TYPE bloom_filter(0.01) GRANULARITY 4;

OPTIMIZE TABLE bad_skip_idx FINAL;

SELECT count(*) from bad_skip_idx WHERE value = 'xxxxxxxxxx1015'; -- check no exception

DROP STREAM IF EXISTS bad_skip_idx;
