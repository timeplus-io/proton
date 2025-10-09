SET allow_experimental_analyzer = 1;
SET optimize_move_to_prewhere = 0;
SET query_plan_convert_outer_join_to_inner_join = 0;

DROP STREAM IF EXISTS test_table_1;
CREATE STREAM test_table_1
(
    id uint64,
    value string
) ENGINE=MergeTree ORDER BY id;

CREATE STREAM test_table_2
(
    id uint64,
    value string
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table_1 SELECT number, number FROM numbers(10);
INSERT INTO test_table_2 SELECT number, number FROM numbers(10);

-- { echoOn }

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5 AND rhs.id = 6;

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5 AND rhs.id = 6;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs LEFT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs LEFT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs LEFT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs LEFT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs RIGHT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs RIGHT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs RIGHT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs RIGHT JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE rhs.id = 5;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5 AND rhs.id = 6;

SELECT '--';

SELECT lhs.id, rhs.id, lhs.value, rhs.value FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id
WHERE lhs.id = 5 AND rhs.id = 6;

-- { echoOff }

DROP STREAM test_table_1;
DROP STREAM test_table_2;
