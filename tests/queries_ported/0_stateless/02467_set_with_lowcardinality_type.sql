-- https://github.com/ClickHouse/ClickHouse/issues/42460
DROP STREAM IF EXISTS bloom_filter_nullable_index__fuzz_0;
CREATE STREAM bloom_filter_nullable_index__fuzz_0
(
    `order_key` uint64,
    `str` nullable(string),
    INDEX idx str TYPE bloom_filter GRANULARITY 1
)
ORDER BY order_key SETTINGS index_granularity = 6;
SELECT sleep(1);
INSERT INTO bloom_filter_nullable_index__fuzz_0(order_key, str) VALUES (1, 'test');
INSERT INTO bloom_filter_nullable_index__fuzz_0(order_key, str) VALUES (2, 'test2');

DROP STREAM IF EXISTS bloom_filter_nullable_index__fuzz_1;
CREATE STREAM bloom_filter_nullable_index__fuzz_1
(
    `order_key` uint64,
    `str` string,
    INDEX idx str TYPE bloom_filter GRANULARITY 1
)
ORDER BY order_key SETTINGS index_granularity = 6;

INSERT INTO bloom_filter_nullable_index__fuzz_0(order_key, str) VALUES (1, 'test');
INSERT INTO bloom_filter_nullable_index__fuzz_0(order_key, str) VALUES (2, 'test2');

DROP STREAM IF EXISTS nullable_string_value__fuzz_2;
CREATE STREAM nullable_string_value__fuzz_2 (`value` low_cardinality(string));
SELECT sleep(1);
INSERT INTO nullable_string_value__fuzz_2(value) VALUES ('test');

SELECT sleep(2);
SELECT order_key, str FROM table(bloom_filter_nullable_index__fuzz_0) WHERE str IN (SELECT value FROM table(nullable_string_value__fuzz_2));
SELECT order_key, str FROM table(bloom_filter_nullable_index__fuzz_1) WHERE str IN (SELECT value FROM table(nullable_string_value__fuzz_2));