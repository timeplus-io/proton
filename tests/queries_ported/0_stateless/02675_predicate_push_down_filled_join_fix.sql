SET allow_experimental_analyzer = 1;
SET optimize_move_to_prewhere = 0;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value string
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, 'Value');

DROP STREAM IF EXISTS test_table_join;
CREATE STREAM test_table_join
(
    id uint64,
    value string
) ENGINE = Join(All, inner, id);

INSERT INTO test_table_join VALUES (0, 'JoinValue');

EXPLAIN header = 1, actions = 1 SELECT t1.id, t1.value, t2.value FROM test_table AS t1 INNER JOIN test_table_join AS t2 ON t1.id = t2.id WHERE t1.id = 0;

SELECT t1.id, t1.value, t2.value FROM test_table AS t1 INNER JOIN test_table_join AS t2 ON t1.id = t2.id WHERE t1.id = 0;

DROP STREAM test_table_join;
DROP STREAM test_table;
