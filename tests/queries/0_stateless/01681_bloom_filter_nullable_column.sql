DROP STREAM IF EXISTS bloom_filter_nullable_index;
create stream bloom_filter_nullable_index
    (
        order_key uint64,
        str Nullable(string),

        INDEX idx (str) TYPE bloom_filter GRANULARITY 1
    )
    ENGINE = MergeTree() 
    ORDER BY order_key SETTINGS index_granularity = 6;

INSERT INTO bloom_filter_nullable_index VALUES (1, 'test');
INSERT INTO bloom_filter_nullable_index VALUES (2, 'test2');

SELECT 'NullableTuple with transform_null_in=0';
SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT '1048576', str FROM bloom_filter_nullable_index) SETTINGS transform_null_in = 0;
SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT '1048576', str FROM bloom_filter_nullable_index) SETTINGS transform_null_in = 0;

SELECT 'NullableTuple with transform_null_in=1';

SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT '1048576', str FROM bloom_filter_nullable_index) SETTINGS transform_null_in = 1; -- { serverError 20 }

SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT '1048576', str FROM bloom_filter_nullable_index) SETTINGS transform_null_in = 1; -- { serverError 20 }


SELECT 'NullableColumnFromCast with transform_null_in=0';
SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT cast('test', 'Nullable(string)')) SETTINGS transform_null_in = 0;

SELECT 'NullableColumnFromCast with transform_null_in=1';
SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT cast('test', 'Nullable(string)')) SETTINGS transform_null_in = 1;

DROP STREAM IF EXISTS nullable_string_value;
create stream nullable_string_value (value Nullable(string)) ;
INSERT INTO nullable_string_value VALUES ('test');

SELECT 'NullableColumnFromTable with transform_null_in=0';
SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT value FROM nullable_string_value) SETTINGS transform_null_in = 0;

SELECT 'NullableColumnFromTable with transform_null_in=1';
SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT value FROM nullable_string_value) SETTINGS transform_null_in = 1;

DROP STREAM nullable_string_value; 
DROP STREAM bloom_filter_nullable_index;
