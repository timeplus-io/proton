SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table_join_1;
CREATE STREAM test_table_join_1
(
    id uint64,
    value uint64
) ENGINE=SummingMergeTree(value)
ORDER BY id
SAMPLE BY id;

SYSTEM STOP MERGES test_table_join_1;
INSERT INTO test_table_join_1 VALUES (0, 1), (1, 1);
INSERT INTO test_table_join_1 VALUES (0, 2);

DROP STREAM IF EXISTS test_table_join_2;
CREATE STREAM test_table_join_2
(
    id uint64,
    value uint64
) ENGINE=SummingMergeTree(value)
ORDER BY id
SAMPLE BY id;

SYSTEM STOP MERGES test_table_join_2;
INSERT INTO test_table_join_2 VALUES (0, 1), (1, 1);
INSERT INTO test_table_join_2 VALUES (1, 2);

SELECT t1.id AS t1_id, t2.id AS t2_id, t1.value AS t1_value, t2.value AS t2_value
FROM test_table_join_1 AS t1 FINAL INNER JOIN test_table_join_2 AS t2 FINAL ON t1.id = t2.id
ORDER BY t1_id;

DROP STREAM test_table_join_1;
DROP STREAM test_table_join_2;
