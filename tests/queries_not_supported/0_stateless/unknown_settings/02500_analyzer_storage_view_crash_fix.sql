SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    f1 int32,
    f2 int32,
    pk int32
) ENGINE = MergeTree PARTITION BY pk ORDER BY f1;

INSERT INTO test_table SELECT number, number, number FROM numbers(10);

DROP VIEW IF EXISTS test_view;
CREATE VIEW test_view AS SELECT f1, f2 FROM test_table WHERE pk = 2;

SELECT * FROM test_view;

DROP VIEW test_view;
DROP STREAM test_table;
