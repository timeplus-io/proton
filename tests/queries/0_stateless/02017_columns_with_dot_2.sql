DROP STREAM IF EXISTS test_nested;

create stream test_nested
(
    `id` string,
    `with_dot.str` string,
    `with_dot.array` array(int32)
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_nested VALUES('123', 'asd', [1,2]);
SELECT * FROM test_nested;

ALTER STREAM test_nested ADD COLUMN `with_dot.bool` uint8;
SELECT * FROM test_nested;

DROP STREAM test_nested;
