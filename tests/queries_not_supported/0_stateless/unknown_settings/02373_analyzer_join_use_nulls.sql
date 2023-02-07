SET allow_experimental_analyzer = 1;
SET join_use_nulls = 1;

DROP STREAM IF EXISTS test_table_join_1;
CREATE STREAM test_table_join_1
(
    id uint64,
    value string
) ENGINE = TinyLog;

DROP STREAM IF EXISTS test_table_join_2;
CREATE STREAM test_table_join_2
(
    id uint64,
    value string
) ENGINE = TinyLog;

INSERT INTO test_table_join_1 VALUES (0, 'Join_1_Value_0');
INSERT INTO test_table_join_1 VALUES (1, 'Join_1_Value_1');
INSERT INTO test_table_join_1 VALUES (2, 'Join_1_Value_2');

INSERT INTO test_table_join_2 VALUES (0, 'Join_2_Value_0');
INSERT INTO test_table_join_2 VALUES (1, 'Join_2_Value_1');
INSERT INTO test_table_join_2 VALUES (3, 'Join_2_Value_3');

-- { echoOn }

SELECT t1.id AS t1_id, to_type_name(t1_id), t1.value AS t1_value, to_type_name(t1_value), t2.id AS t2_id, to_type_name(t2_id), t2.value AS t2_value, to_type_name(t2_value)
FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id;

SELECT '--';

SELECT t1.id AS t1_id, to_type_name(t1_id), t1.value AS t1_value, to_type_name(t1_value), t2.id AS t2_id, to_type_name(t2_id), t2.value AS t2_value, to_type_name(t2_value)
FROM test_table_join_1 AS t1 LEFT JOIN test_table_join_2 AS t2 ON t1.id = t2.id;

SELECT '--';

SELECT t1.id AS t1_id, to_type_name(t1_id), t1.value AS t1_value, to_type_name(t1_value), t2.id AS t2_id, to_type_name(t2_id), t2.value AS t2_value, to_type_name(t2_value)
FROM test_table_join_1 AS t1 RIGHT JOIN test_table_join_2 AS t2 ON t1.id = t2.id;

SELECT '--';

SELECT t1.id AS t1_id, to_type_name(t1_id), t1.value AS t1_value, to_type_name(t1_value), t2.id AS t2_id, to_type_name(t2_id), t2.value AS t2_value, to_type_name(t2_value)
FROM test_table_join_1 AS t1 FULL JOIN test_table_join_2 AS t2 ON t1.id = t2.id;

SELECT '--';

SELECT id AS using_id, to_type_name(using_id), t1.id AS t1_id, to_type_name(t1_id), t1.value AS t1_value, to_type_name(t1_value),
t2.id AS t2_id, to_type_name(t2_id), t2.value AS t2_value, to_type_name(t2_value)
FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id);

SELECT '--';

SELECT id AS using_id, to_type_name(using_id), t1.id AS t1_id, to_type_name(t1_id), t1.value AS t1_value, to_type_name(t1_value),
t2.id AS t2_id, to_type_name(t2_id), t2.value AS t2_value, to_type_name(t2_value)
FROM test_table_join_1 AS t1 LEFT JOIN test_table_join_2 AS t2 USING (id);

SELECT '--';

SELECT id AS using_id, to_type_name(using_id), t1.id AS t1_id, to_type_name(t1_id), t1.value AS t1_value, to_type_name(t1_value),
t2.id AS t2_id, to_type_name(t2_id), t2.value AS t2_value, to_type_name(t2_value)
FROM test_table_join_1 AS t1 RIGHT JOIN test_table_join_2 AS t2 USING (id);

SELECT '--';

SELECT id AS using_id, to_type_name(using_id), t1.id AS t1_id, to_type_name(t1_id), t1.value AS t1_value, to_type_name(t1_value),
t2.id AS t2_id, to_type_name(t2_id), t2.value AS t2_value, to_type_name(t2_value)
FROM test_table_join_1 AS t1 FULL JOIN test_table_join_2 AS t2 USING (id);

-- { echoOff }

DROP STREAM test_table_join_1;
DROP STREAM test_table_join_2;
