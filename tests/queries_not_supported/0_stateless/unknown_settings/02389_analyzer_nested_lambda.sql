SET allow_experimental_analyzer = 1;

-- { echoOn }

SELECT array_map(x -> x + array_map(x -> x + 1, [1])[1], [1,2,3]);

SELECT '--';

SELECT array_map(x -> x + array_map(x -> 5, [1])[1], [1,2,3]);

SELECT '--';

SELECT 5 AS constant, array_map(x -> x + array_map(x -> constant, [1])[1], [1,2,3]);

SELECT '--';

SELECT array_map(x -> x + array_map(x -> x, [1])[1], [1,2,3]);

SELECT '--';

SELECT array_map(x -> x + array_map(y -> x + y, [1])[1], [1,2,3]);

SELECT '--';

SELECT array_map(x -> x + array_map(x -> (SELECT 5), [1])[1], [1,2,3]);

SELECT '--';

SELECT (SELECT 5) AS subquery, array_map(x -> x + array_map(x -> subquery, [1])[1], [1,2,3]);

SELECT '--';

SELECT array_map(x -> x + array_map(x -> (SELECT 5 UNION DISTINCT SELECT 5), [1])[1], [1,2,3]);

SELECT '--';

SELECT (SELECT 5 UNION DISTINCT SELECT 5) AS subquery, array_map(x -> x + array_map(x -> subquery, [1])[1], [1,2,3]);

SELECT '--';

WITH x -> to_string(x) AS lambda SELECT array_map(x -> lambda(x), [1,2,3]);

SELECT '--';

WITH x -> to_string(x) AS lambda SELECT array_map(x -> array_map(y -> concat(lambda(x), '_', lambda(y)), [1,2,3]), [1,2,3]);

SELECT '--';

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value string
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

SELECT array_map(x -> x + array_map(x -> id, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT array_map(x -> x + array_map(x -> x + id, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT array_map(x -> x + array_map(y -> x + y + id, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT id AS id_alias, array_map(x -> x + array_map(y -> x + y + id_alias, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT array_map(x -> x + array_map(x -> 5, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT 5 AS constant, array_map(x -> x + array_map(x -> constant, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT 5 AS constant, array_map(x -> x + array_map(x -> x + constant, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT 5 AS constant, array_map(x -> x + array_map(x -> x + id + constant, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT 5 AS constant, array_map(x -> x + array_map(y -> x + y + id + constant, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT array_map(x -> x + array_map(x -> id + (SELECT id FROM test_table), [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT array_map(x -> id + array_map(x -> id + (SELECT id FROM test_table), [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT array_map(x -> id + array_map(x -> id + (SELECT id FROM test_table UNION DISTINCT SELECT id FROM test_table), [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

WITH x -> to_string(id) AS lambda SELECT array_map(x -> lambda(x), [1,2,3]) FROM test_table;

SELECT '--';

WITH x -> to_string(id) AS lambda SELECT array_map(x -> array_map(y -> lambda(y), [1,2,3]), [1,2,3]) FROM test_table;

SELECT '--';

WITH x -> to_string(id) AS lambda SELECT array_map(x -> array_map(y -> concat(lambda(x), '_', lambda(y)), [1,2,3]), [1,2,3]) FROM test_table;

SELECT '--';

SELECT array_map(x -> concat(concat(concat(concat(concat(to_string(id), '___\0_______\0____'), to_string(id), concat(concat(to_string(id), ''), to_string(id)), to_string(id)),
    array_map(x -> concat(concat(concat(concat(to_string(id), ''), to_string(id)), to_string(id), '___\0_______\0____'), to_string(id)) AS lambda, [NULL, inf, 1, 1]),
    concat(to_string(id), NULL), to_string(id)), to_string(id))) AS lambda, [NULL, NULL, 2147483647])
FROM test_table WHERE concat(concat(concat(to_string(id), '___\0_______\0____'), to_string(id)), concat(to_string(id), NULL), to_string(id));

SELECT '--';

SELECT array_map(x -> concat(to_string(id), array_map(x -> to_string(1), [NULL])), [NULL]) FROM test_table; -- { serverError 44 };

DROP STREAM test_table;

-- { echoOff }
