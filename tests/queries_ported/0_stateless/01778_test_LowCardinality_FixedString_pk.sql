DROP STREAM IF EXISTS test_01778;


CREATE STREAM test_01778
(
    `key` low_cardinality(fixed_string(3)),
    `d` date
)
ENGINE = MergeTree(d, key, 8192);


INSERT INTO test_01778 SELECT to_string(int_div(number,8000)), today() FROM numbers(100000);
INSERT INTO test_01778 SELECT to_string('xxx'), today() FROM numbers(100);

SELECT count() FROM test_01778 WHERE key = 'xxx';

SELECT count() FROM test_01778 WHERE key = to_fixed_string('xxx', 3);

SELECT count() FROM test_01778 WHERE to_string(key) = 'xxx';

DROP STREAM test_01778;

