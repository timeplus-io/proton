DROP STREAM IF EXISTS invalid_min_index_granularity_bytes_setting;

CREATE STREAM invalid_min_index_granularity_bytes_setting
(
  id uint64,
  value string
) ENGINE MergeTree()
ORDER BY id SETTINGS index_granularity_bytes = 1, min_index_granularity_bytes = 1024; -- { serverError 36 }

DROP STREAM IF EXISTS valid_min_index_granularity_bytes_setting;

CREATE STREAM valid_min_index_granularity_bytes_setting
(
  id uint64,
  value string
) ENGINE MergeTree()
ORDER BY id SETTINGS index_granularity_bytes = 2024, min_index_granularity_bytes = 1024;

INSERT INTO valid_min_index_granularity_bytes_setting SELECT number, concat('xxxxxxxxxx', to_string(number)) FROM numbers(1000,1000);

SELECT count(*) from valid_min_index_granularity_bytes_setting WHERE value = 'xxxxxxxxxx1015';

DROP STREAM IF EXISTS valid_min_index_granularity_bytes_setting;
