SET check_query_single_value_result = 'false';

DROP STREAM IF EXISTS check_table_with_indices;

CREATE STREAM check_table_with_indices (
  id uint64,
  data string,
  INDEX a (id) type minmax GRANULARITY 3
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO check_table_with_indices VALUES (0, 'test'), (1, 'test2');

CHECK TABLE check_table_with_indices;

DROP STREAM check_table_with_indices;
