SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table_join_1;
CREATE STREAM test_table_join_1
(
    id uint8,
    value string
)
ENGINE = TinyLog;

INSERT INTO test_table_join_1 VALUES (0, 'Value_0');

DROP STREAM IF EXISTS test_table_join_2;
CREATE STREAM test_table_join_2
(
    id uint16,
    value string
)
ENGINE = TinyLog;

INSERT INTO test_table_join_2 VALUES (0, 'Value_1');

SELECT
    to_type_name(t2_value),
    t2.value AS t2_value
FROM test_table_join_1 AS t1
INNER JOIN test_table_join_2 USING (id); -- { serverError 47 };

SELECT
    to_type_name(t2_value),
    t2.value AS t2_value
FROM test_table_join_1 AS t1
INNER JOIN test_table_join_2 AS t2 USING (id);

DROP STREAM test_table_join_1;
DROP STREAM test_table_join_2;
