SET allow_experimental_analyzer = 1;

EXPLAIN QUERY TREE run_passes = 0 SELECT 1;

SELECT '--';

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value string
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

EXPLAIN QUERY TREE run_passes = 0 SELECT id, value FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 1 SELECT id, value FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 0 SELECT array_map(x -> x + id, [1, 2, 3]) FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 1 SELECT array_map(x -> x + 1, [1, 2, 3]) FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 0 WITH x -> x + 1 AS lambda SELECT lambda(id) FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 1 WITH x -> x + 1 AS lambda SELECT lambda(id) FROM test_table;

DROP STREAM test_table;
