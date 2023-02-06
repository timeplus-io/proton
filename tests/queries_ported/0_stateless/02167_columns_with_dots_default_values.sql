DROP STREAM IF EXISTS test_nested_default;

CREATE STREAM test_nested_default
(
    `id` string,
    `with_dot.str` string,
    `with_dot.array` array(string)
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_nested_default(`id`, `with_dot.array`) VALUES('id', ['str1', 'str2']);
SELECT * FROM test_nested_default;

DROP STREAM test_nested_default;
