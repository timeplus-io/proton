DROP STREAM IF EXISTS test;

create stream test (col int8) ENGINE=MergeTree ORDER BY tuple()
SETTINGS vertical_merge_algorithm_min_rows_to_activate=1,
         vertical_merge_algorithm_min_columns_to_activate=1,
         min_bytes_for_wide_part = 0;


INSERT INTO test VALUES (1);
ALTER STREAM test ADD COLUMN s1 string;
ALTER STREAM test ADD COLUMN s2 string DEFAULT s1;

OPTIMIZE STREAM test FINAL;

SELECT * FROM test;

DROP STREAM IF EXISTS test;
