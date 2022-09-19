DROP STREAM IF EXISTS test;

create stream test(key FixedString(10)) ENGINE=MergeTree() PARTITION BY tuple() ORDER BY (key);
INSERT INTO test SELECT to_string(int_div(number, 8)) FROM numbers(100);
SELECT count() FROM test WHERE key = '1';

DROP STREAM IF EXISTS test;
