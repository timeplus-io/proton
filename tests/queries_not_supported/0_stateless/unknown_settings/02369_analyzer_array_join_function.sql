SET allow_experimental_analyzer = 1;

SELECT array_join([1, 2, 3]);

SELECT '--';

SELECT array_join([1, 2, 3]) AS a, array_join([1, 2, 3]);

SELECT '--';

SELECT array_join([1, 2, 3]) AS a, a;

SELECT '--';

SELECT array_join([[1, 2, 3]]) AS a, array_join(a) AS b;

SELECT '--';

SELECT array_join([1, 2, 3]) AS a, array_join([1, 2, 3, 4]) AS b;

SELECT '--';

SELECT array_map(x -> array_join([1, 2, 3]), [1, 2, 3]);

SELECT array_map(x -> array_join(x), [[1, 2, 3]]); -- { serverError 36 }

SELECT array_map(x -> array_join(cast(x, 'array(uint8)')), [[1, 2, 3]]); -- { serverError 36 }

SELECT '--';

SELECT array_map(x -> x + a, [1, 2, 3]), array_join([1,2,3]) as a;

SELECT '--';

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value_1 array(uint8),
    value_2 array(uint8),
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, [1, 2, 3], [1, 2, 3, 4]);

SELECT id, array_join(value_1) FROM test_table;

SELECT '--';

SELECT id, array_join(value_1) AS a, a FROM test_table;

-- SELECT '--';

-- SELECT id, array_join(value_1), array_join(value_2) FROM test_table;

-- SELECT '--';

-- SELECT id, array_join(value_1), array_join(value_2), array_join([5, 6]) FROM test_table;

DROP STREAM test_table;
