SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table VALUES (1);

SELECT * FROM test_table WHERE id = 1;

SELECT * FROM test_table WHERE id = 1 SETTINGS query_plan_optimize_primary_key = 0;

DROP STREAM test_table;
