DROP STREAM IF EXISTS test_00974;

create stream test_00974
(
    date date,
    x int32,
    ver uint64
)
ENGINE = ReplacingMergeTree(date, x, 1);

INSERT INTO test_00974 VALUES ('2019-07-23', 1, 1), ('2019-07-23', 1, 2);
INSERT INTO test_00974 VALUES ('2019-07-23', 2, 1), ('2019-07-23', 2, 2);

SELECT COUNT() FROM (SELECT * FROM test_00974 FINAL) where x = 1 SETTINGS enable_optimize_predicate_expression_to_final_subquery = 0;
SELECT COUNT() FROM (SELECT * FROM test_00974 FINAL) where x = 1 SETTINGS enable_optimize_predicate_expression_to_final_subquery = 1, max_rows_to_read = 2;

DROP STREAM test_00974;
