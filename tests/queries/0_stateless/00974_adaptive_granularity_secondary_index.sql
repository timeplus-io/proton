
DROP STREAM IF EXISTS indexed_table;

create stream indexed_table
(
    `tm` DateTime,
    `log_message` string,
    INDEX log_message log_message TYPE tokenbf_v1(4096, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tm)
SETTINGS index_granularity_bytes = 50, min_index_granularity_bytes = 40;

INSERT INTO indexed_table SELECT to_datetime('2019-05-27 10:00:00') + number % 100, 'h' FROM numbers(1000);

INSERT INTO indexed_table
SELECT
    to_datetime('2019-05-27 10:00:00') + number % 100,
    concat('hhhhhhhhhhhhhhhhhhhhhhhhh', 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx', 'yyyyyyyyyyyyyyyyyyyyyyyyyy', to_string(rand()))
FROM numbers(1000);

OPTIMIZE STREAM indexed_table FINAL;

SELECT COUNT() FROM indexed_table WHERE log_message like '%x%';

DROP STREAM IF EXISTS indexed_table;

DROP STREAM IF EXISTS another_indexed_table;

create stream another_indexed_table
(
  `tm` DateTime,
  `log_message` string,
  INDEX log_message log_message TYPE tokenbf_v1(4096, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tm)
SETTINGS index_granularity_bytes = 50,
         min_index_granularity_bytes = 40,
         vertical_merge_algorithm_min_rows_to_activate=0,
         vertical_merge_algorithm_min_columns_to_activate=0;


INSERT INTO another_indexed_table SELECT to_datetime('2019-05-27 10:00:00') + number % 100, 'h' FROM numbers(1000);

INSERT INTO another_indexed_table
SELECT
  to_datetime('2019-05-27 10:00:00') + number % 100,
  concat('hhhhhhhhhhhhhhhhhhhhhhhhh', 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx', 'yyyyyyyyyyyyyyyyyyyyyyyyyy', to_string(rand()))
  FROM numbers(1000);

OPTIMIZE STREAM another_indexed_table FINAL;

SELECT COUNT() FROM another_indexed_table WHERE log_message like '%x%';

DROP STREAM IF EXISTS another_indexed_table;
