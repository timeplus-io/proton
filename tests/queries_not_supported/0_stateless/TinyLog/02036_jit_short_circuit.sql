SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;
SET short_circuit_function_evaluation='enable';

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table (message string) ENGINE=TinyLog;

INSERT INTO test_table VALUES ('Test');

SELECT if(action = 'bonus', sport_amount, 0) * 100 FROM (SELECT message AS action, cast(message, 'float64') AS sport_amount FROM test_table);

DROP STREAM test_table;
