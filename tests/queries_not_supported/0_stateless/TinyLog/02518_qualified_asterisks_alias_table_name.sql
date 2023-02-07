DROP STREAM IF EXISTS test_table_join_1;
CREATE STREAM test_table_join_1 (id uint64, value string) ENGINE = TinyLog;

DROP STREAM IF EXISTS test_table_join_2;
CREATE STREAM test_table_join_2 (id uint64, value string) ENGINE = TinyLog;

DROP STREAM IF EXISTS test_table_join_3;
CREATE STREAM test_table_join_3 (id uint64, value string ) ENGINE = TinyLog;

INSERT INTO test_table_join_1 VALUES (1, 'a');
INSERT INTO test_table_join_2 VALUES (1, 'b');
INSERT INTO test_table_join_3 VALUES (1, 'c');


SELECT
    test_table_join_1.* APPLY to_string,
    test_table_join_2.* APPLY to_string,
    test_table_join_3.* APPLY to_string
FROM test_table_join_1 AS t1
         INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id
         INNER JOIN test_table_join_3 AS t3 ON t2.id = t3.id;

DROP STREAM test_table_join_1;
DROP STREAM test_table_join_2;
DROP STREAM test_table_join_3;