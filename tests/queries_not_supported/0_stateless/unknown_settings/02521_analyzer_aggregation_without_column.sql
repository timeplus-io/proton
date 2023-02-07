SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    c0 string ALIAS c1,
    c1 string,
    c2 string,
) ENGINE = MergeTree ORDER BY c1;

INSERT INTO test_table VALUES ('a', 'b');

SELECT MAX(1) FROM test_table;

DROP STREAM test_table;
