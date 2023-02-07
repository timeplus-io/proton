SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value string
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

SELECT 'Standalone lambdas';

WITH x -> x + 1 AS lambda SELECT lambda(1);
WITH x -> to_string(x) AS lambda SELECT lambda(1), lambda(NULL), lambda([1,2,3]);
WITH x -> to_string(x) AS lambda_1, lambda_1 AS lambda_2, lambda_2 AS lambda_3 SELECT lambda_1(1), lambda_2(NULL), lambda_3([1,2,3]);

WITH x -> x + 1 AS lambda SELECT lambda(id) FROM test_table;
WITH x -> to_string(x) AS lambda SELECT lambda(id), lambda(value) FROM test_table;

SELECT 'Lambda as function parameter';

SELECT array_map(x -> x + 1, [1,2,3]);
WITH x -> x + 1 AS lambda SELECT array_map(lambda, [1,2,3]);
SELECT array_map((x -> to_string(x)) as lambda, [1,2,3]), array_map(lambda, ['1','2','3']);
WITH x -> to_string(x) AS lambda_1 SELECT array_map(lambda_1 AS lambda_2, [1,2,3]), array_map(lambda_2, ['1', '2', '3']);

SELECT array_map(x -> id, [1,2,3]) FROM test_table;
SELECT array_map(x -> x + id, [1,2,3]) FROM test_table;
SELECT array_map((x -> concat(concat(to_string(x), '_'), to_string(id))) as lambda, [1,2,3]) FROM test_table;

SELECT 'Lambda compound argument';

DROP STREAM IF EXISTS test_table_tuple;
CREATE STREAM test_table_tuple
(
    id uint64,
    value Tuple(value_0_level_0 string, value_1_level_0 string)
) ENGINE=TinyLog;

INSERT INTO test_table_tuple VALUES (0, ('value_0_level_0', 'value_1_level_0'));

WITH x -> concat(concat(to_string(x.id), '_'), x.value) AS lambda SELECT cast((1, 'Value'), 'Tuple (id uint64, value string)') AS value, lambda(value);
WITH x -> concat(concat(x.value_0_level_0, '_'), x.value_1_level_0) AS lambda SELECT lambda(value) FROM test_table_tuple;

SELECT 'Lambda matcher';

WITH x -> * AS lambda SELECT lambda(1);
WITH x -> * AS lambda SELECT lambda(1) FROM test_table;

WITH cast(tuple(1), 'Tuple (value uint64)') AS compound_value SELECT array_map(x -> compound_value.*, [1,2,3]);
WITH cast(tuple(1, 1), 'Tuple (value_1 uint64, value_2 uint64)') AS compound_value SELECT array_map(x -> compound_value.*, [1,2,3]); -- { serverError 1 }
WITH cast(tuple(1, 1), 'Tuple (value_1 uint64, value_2 uint64)') AS compound_value SELECT array_map(x -> plus(compound_value.*), [1,2,3]);

WITH cast(tuple(1), 'Tuple (value uint64)') AS compound_value SELECT id, test_table.* APPLY x -> compound_value.* FROM test_table;
WITH cast(tuple(1, 1), 'Tuple (value_1 uint64, value_2 uint64)') AS compound_value SELECT id, test_table.* APPLY x -> compound_value.* FROM test_table; -- { serverError 1 }
WITH cast(tuple(1, 1), 'Tuple (value_1 uint64, value_2 uint64)') AS compound_value SELECT id, test_table.* APPLY x -> plus(compound_value.*) FROM test_table;

SELECT 'Lambda untuple';

WITH x -> untuple(x) AS lambda SELECT cast((1, 'Value'), 'Tuple (id uint64, value string)') AS value, lambda(value);

SELECT 'Lambda carrying';

WITH (functor, x) -> functor(x) AS lambda, x -> x + 1 AS functor_1, x -> to_string(x) AS functor_2 SELECT lambda(functor_1, 1), lambda(functor_2, 1);
WITH (functor, x) -> functor(x) AS lambda, x -> x + 1 AS functor_1, x -> to_string(x) AS functor_2 SELECT lambda(functor_1, id), lambda(functor_2, id) FROM test_table;

DROP STREAM test_table_tuple;
DROP STREAM test_table;
