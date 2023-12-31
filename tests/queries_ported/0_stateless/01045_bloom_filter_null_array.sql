DROP STREAM IF EXISTS bloom_filter_null_array;

CREATE STREAM bloom_filter_null_array (v array(low_cardinality(nullable(string))), INDEX idx v TYPE bloom_filter(0.1) GRANULARITY 1) ENGINE = MergeTree() ORDER BY v SETTINGS allow_nullable_key = 1;

INSERT INTO bloom_filter_null_array VALUES ([]);
INSERT INTO bloom_filter_null_array VALUES (['1', '2']) ([]) ([]);
INSERT INTO bloom_filter_null_array VALUES ([]) ([]) (['2', '3']);

SELECT count() FROM bloom_filter_null_array;
SELECT count() FROM bloom_filter_null_array WHERE has(v, '1');
SELECT count() FROM bloom_filter_null_array WHERE has(v, '2');
SELECT count() FROM bloom_filter_null_array WHERE has(v, '3');
SELECT count() FROM bloom_filter_null_array WHERE has(v, '4');

DROP STREAM IF EXISTS bloom_filter_null_array;
