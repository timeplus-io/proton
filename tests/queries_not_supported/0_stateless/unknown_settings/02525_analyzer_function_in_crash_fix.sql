SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint8,
    value nullable(decimal(38, 2))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table VALUES (1, '22.5'), (2, Null);

SELECT id IN to_decimal64(257, NULL) FROM test_table;

DROP STREAM test_table;
