DROP STREAM IF EXISTS test_vertical_merge;

create stream test_vertical_merge (
  k uint64,
  val1 uint64,
  val2 uint64,
  INDEX idx1 val1 * val2 TYPE minmax GRANULARITY 1,
  INDEX idx2 val1 * k TYPE minmax GRANULARITY 1
) ENGINE MergeTree()
ORDER BY k
SETTINGS vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO test_vertical_merge SELECT number, number + 5, number * 12 from numbers(1000);

SELECT count() from test_vertical_merge WHERE val2 <= 2400;

OPTIMIZE TABLE test_vertical_merge FINAL;

SELECT count() from test_vertical_merge WHERE val2 <= 2400;

DROP STREAM IF EXISTS test_vertical_merge;
