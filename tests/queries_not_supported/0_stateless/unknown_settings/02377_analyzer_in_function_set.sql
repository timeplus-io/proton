SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value string
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value_0'), (1, 'Value_1'), (2, 'Value_2');

DROP STREAM IF EXISTS special_set_table;
CREATE STREAM special_set_table
(
    id uint64
) ENGINE=Set;

INSERT INTO special_set_table VALUES (0), (1);

SELECT id, value FROM test_table WHERE id IN special_set_table;

DROP STREAM special_set_table;
DROP STREAM test_table;
