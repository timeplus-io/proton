SET allow_experimental_analyzer = 1;
SET join_algorithm = 'hash';

DROP STREAM IF EXISTS test_STREAM_1;
CREATE STREAM test_STREAM_1
(
    id uint64,
    value string
) ENGINE=MergeTree ORDER BY id;

DROP STREAM IF EXISTS test_STREAM_2;
CREATE STREAM test_STREAM_2
(
    id uint64,
    value string
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_STREAM_1 VALUES (1, 'Value_1'), (2, 'Value_2');
INSERT INTO test_STREAM_2 VALUES (2, 'Value_2'), (3, 'Value_3');

EXPLAIN header = 1, actions = 1 SELECT * FROM test_STREAM_1 AS lhs LEFT JOIN test_STREAM_2 AS rhs ON lhs.id = rhs.id WHERE rhs.id != 0;

SELECT '--';

SELECT * FROM test_STREAM_1 AS lhs LEFT JOIN test_STREAM_2 AS rhs ON lhs.id = rhs.id WHERE rhs.id != 0;

SELECT '--';

EXPLAIN header = 1, actions = 1 SELECT * FROM test_STREAM_1 AS lhs RIGHT JOIN test_STREAM_2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0;

SELECT '--';

SELECT * FROM test_STREAM_1 AS lhs RIGHT JOIN test_STREAM_2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0;

SELECT '--';

EXPLAIN header = 1, actions = 1 SELECT * FROM test_STREAM_1 AS lhs FULL JOIN test_STREAM_2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0 AND rhs.id != 0;

SELECT '--';

SELECT * FROM test_STREAM_1 AS lhs FULL JOIN test_STREAM_2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0 AND rhs.id != 0;

DROP STREAM test_STREAM_1;
DROP STREAM test_STREAM_2;
