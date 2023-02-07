SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table_join_1;
CREATE STREAM test_table_join_1
(
    id uint8,
    value string
) ENGINE = TinyLog;

DROP STREAM IF EXISTS test_table_join_2;
CREATE STREAM test_table_join_2
(
    id uint16,
    value string
) ENGINE = TinyLog;

DROP STREAM IF EXISTS test_table_join_3;
CREATE STREAM test_table_join_3
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

INSERT INTO test_table_join_3 VALUES (0, 'Join_3_Value_0');
INSERT INTO test_table_join_3 VALUES (1, 'Join_3_Value_1');
INSERT INTO test_table_join_3 VALUES (4, 'Join_3_Value_4');

-- { echoOn }

SELECT * FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id) ORDER BY id, t1.value;

SELECT * FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id, id, id) ORDER BY id, t1.value; -- { serverError 36 }

SELECT '--';

SELECT * FROM test_table_join_1 AS t1 LEFT JOIN test_table_join_2 AS t2 USING (id) ORDER BY id, t1.value;

SELECT '--';

SELECT * FROM test_table_join_1 AS t1 RIGHT JOIN test_table_join_2 AS t2 USING (id) ORDER BY id, t1.value;

SELECT '--';

SELECT * FROM test_table_join_1 AS t1 FULL JOIN test_table_join_2 AS t2 USING (id) ORDER BY id, t1.value;

SELECT '--';

SELECT * FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id) INNER JOIN test_table_join_3 AS t3 USING (id) ORDER BY id, t1.value;

SELECT '--';

SELECT * FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id) LEFT JOIN test_table_join_3 AS t3 USING (id) ORDER BY id, t1.value;

SELECT '--';

SELECT * FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id) RIGHT JOIN test_table_join_3 AS t3 USING (id) ORDER BY id, t1.value;

SELECT '--';

SELECT * FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id) FULL JOIN test_table_join_3 AS t3 USING (id) ORDER BY id, t1.value;

-- { echoOff }

DROP STREAM test_table_join_1;
DROP STREAM test_table_join_2;
DROP STREAM test_table_join_3;
