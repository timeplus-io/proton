SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table_join_1;
CREATE STREAM test_table_join_1
(
    id uint64,
    value string
) ENGINE=MergeTree
ORDER BY id
SAMPLE BY id;

INSERT INTO test_table_join_1 VALUES (0, 'Value'), (1, 'Value_1');

DROP STREAM IF EXISTS test_table_join_2;
CREATE STREAM test_table_join_2
(
    id uint64,
    value string
) ENGINE=MergeTree
ORDER BY id
SAMPLE BY id;

INSERT INTO test_table_join_2 VALUES (0, 'Value'), (1, 'Value_1');

SELECT t1.id AS t1_id, t2.id AS t2_id, t1._sample_factor AS t1_sample_factor, t2._sample_factor AS t2_sample_factor
FROM test_table_join_1 AS t1 SAMPLE 1/2 INNER JOIN test_table_join_2 AS t2 SAMPLE 1/2 ON t1.id = t2.id;

DROP STREAM test_table_join_1;
DROP STREAM test_table_join_2;
