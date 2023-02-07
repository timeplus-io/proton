-- https://github.com/ClickHouse/ClickHouse/issues/42460
DROP STREAM IF EXISTS bloom_filter_nullable_index__fuzz_0;
CREATE STREAM bloom_filter_nullable_index__fuzz_0
(
    `order_key` uint64,
    `str` nullable(string),
    INDEX idx str TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY order_key SETTINGS index_granularity = 6;

INSERT INTO bloom_filter_nullable_index__fuzz_0 VALUES (1, 'test');
INSERT INTO bloom_filter_nullable_index__fuzz_0 VALUES (2, 'test2');

DROP STREAM IF EXISTS bloom_filter_nullable_index__fuzz_1;
CREATE STREAM bloom_filter_nullable_index__fuzz_1
(
    `order_key` uint64,
    `str` string,
    INDEX idx str TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY order_key SETTINGS index_granularity = 6;

INSERT INTO bloom_filter_nullable_index__fuzz_0 VALUES (1, 'test');
INSERT INTO bloom_filter_nullable_index__fuzz_0 VALUES (2, 'test2');

DROP STREAM IF EXISTS nullable_string_value__fuzz_2;
CREATE STREAM nullable_string_value__fuzz_2 (`value` low_cardinality(string)) ENGINE = TinyLog;
INSERT INTO nullable_string_value__fuzz_2 VALUES ('test');

SELECT * FROM bloom_filter_nullable_index__fuzz_0 WHERE str IN (SELECT value FROM nullable_string_value__fuzz_2);
SELECT * FROM bloom_filter_nullable_index__fuzz_1 WHERE str IN (SELECT value FROM nullable_string_value__fuzz_2);
