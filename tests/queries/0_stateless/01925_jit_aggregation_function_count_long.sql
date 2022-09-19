-- Tags: long

SET compile_aggregate_expressions = 1;
SET min_count_to_compile_aggregate_expression = 0;

DROP STREAM IF EXISTS test_table;
create stream test_table
(
    id uint64,

    value uint8,
    value_nullable Nullable(uint8)
) ;

INSERT INTO test_table SELECT number % 3, number, if (number % 2 == 0, number, NULL) FROM system.numbers LIMIT 120;
SELECT id, count(value), count(value_nullable) FROM test_table GROUP BY id ORDER BY id;
DROP STREAM test_table;
