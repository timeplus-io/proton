
DROP STREAM IF EXISTS single_column_bloom_filter;

create stream single_column_bloom_filter (u64 uint64, i32 int32, i64 uint64, INDEX idx (i32) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 6;

INSERT INTO single_column_bloom_filter SELECT number AS u64, number AS i32, number AS i64 FROM system.numbers LIMIT 100;

SELECT count() FROM single_column_bloom_filter WHERE i32 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM single_column_bloom_filter WHERE (i32, i32) = (1, 2) SETTINGS max_rows_to_read = 6;
SELECT count() FROM single_column_bloom_filter WHERE (i32, i64) = (1, 1) SETTINGS max_rows_to_read = 6;
SELECT count() FROM single_column_bloom_filter WHERE (i64, (i64, i32)) = (1, (1, 1)) SETTINGS max_rows_to_read = 6;

SELECT count() FROM single_column_bloom_filter WHERE i32 IN (1, 2) SETTINGS max_rows_to_read = 6;
SELECT count() FROM single_column_bloom_filter WHERE (i32, i32) IN ((1, 2), (2, 3)) SETTINGS max_rows_to_read = 6;
SELECT count() FROM single_column_bloom_filter WHERE (i32, i64) IN ((1, 1), (2, 2)) SETTINGS max_rows_to_read = 6;
SELECT count() FROM single_column_bloom_filter WHERE (i64, (i64, i32)) IN ((1, (1, 1)), (2, (2, 2))) SETTINGS max_rows_to_read = 6;
SELECT count() FROM single_column_bloom_filter WHERE i32 IN (SELECT array_join([to_int32(1), to_int32(2)])) SETTINGS max_rows_to_read = 7;
SELECT count() FROM single_column_bloom_filter WHERE (i32, i32) IN (SELECT array_join([(to_int32(1), to_int32(2)), (to_int32(2), to_int32(3))])) SETTINGS max_rows_to_read = 7;
SELECT count() FROM single_column_bloom_filter WHERE (i32, i64) IN (SELECT array_join([(to_int32(1), to_uint64(1)), (to_int32(2), to_uint64(2))])) SETTINGS max_rows_to_read = 7;
SELECT count() FROM single_column_bloom_filter WHERE (i64, (i64, i32)) IN (SELECT array_join([(to_uint64(1), (to_uint64(1), to_int32(1))), (to_uint64(2), (to_uint64(2), to_int32(2)))])) SETTINGS max_rows_to_read = 7;
WITH (1, 2) AS liter_prepared_set SELECT count() FROM single_column_bloom_filter WHERE i32 IN liter_prepared_set SETTINGS max_rows_to_read = 6;
WITH ((1, 2), (2, 3)) AS liter_prepared_set SELECT count() FROM single_column_bloom_filter WHERE (i32, i32) IN liter_prepared_set SETTINGS max_rows_to_read = 6;
WITH ((1, 1), (2, 2)) AS liter_prepared_set SELECT count() FROM single_column_bloom_filter WHERE (i32, i64) IN liter_prepared_set SETTINGS max_rows_to_read = 6;
WITH ((1, (1, 1)), (2, (2, 2))) AS liter_prepared_set SELECT count() FROM single_column_bloom_filter WHERE (i64, (i64, i32)) IN liter_prepared_set SETTINGS max_rows_to_read = 6;

DROP STREAM IF EXISTS single_column_bloom_filter;


DROP STREAM IF EXISTS bloom_filter_types_test;

create stream bloom_filter_types_test (order_key   uint64, i8 int8, i16 int16, i32 int32, i64 int64, u8 uint8, u16 uint16, u32 uint32, u64 uint64, f32 float32, f64 float64, date date, date_time datetime('Europe/Moscow'), str string, fixed_string fixed_string(5), INDEX idx (i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, date, date_time, str, fixed_string) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree() ORDER BY order_key SETTINGS index_granularity = 6;
INSERT INTO bloom_filter_types_test SELECT number AS order_key, to_int8(number) AS i8, to_int16(number) AS i16, to_int32(number) AS i32, to_int64(number) AS i64, to_uint8(number) AS u8, to_uint16(number) AS u16, to_uint32(number) AS u32, to_uint64(number) AS u64, to_float32(number) AS f32, to_float64(number) AS f64, to_date(number, 'Europe/Moscow') AS date, to_datetime(number, 'Europe/Moscow') AS date_time, to_string(number) AS str, to_fixed_string(to_string(number), 5) AS fixed_string FROM system.numbers LIMIT 100;

SELECT count() FROM bloom_filter_types_test WHERE i8 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_types_test WHERE i16 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_types_test WHERE i32 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_types_test WHERE i64 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_types_test WHERE u8 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_types_test WHERE u16 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_types_test WHERE u32 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_types_test WHERE u64 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_types_test WHERE f32 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_types_test WHERE f64 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_types_test WHERE date = '1970-01-02' SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_types_test WHERE date_time = to_datetime('1970-01-01 03:00:01', 'Europe/Moscow') SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_types_test WHERE str = '1' SETTINGS max_rows_to_read = 12;
SELECT count() FROM bloom_filter_types_test WHERE fixed_string = to_fixed_string('1', 5) SETTINGS max_rows_to_read = 12;

SELECT count() FROM bloom_filter_types_test WHERE str IN ( SELECT str FROM bloom_filter_types_test);

DROP STREAM IF EXISTS bloom_filter_types_test;

DROP STREAM IF EXISTS bloom_filter_array_types_test;

create stream bloom_filter_array_types_test (order_key   array(uint64), i8 array(int8), i16 array(int16), i32 array(int32), i64 array(int64), u8 array(uint8), u16 array(uint16), u32 array(uint32), u64 array(uint64), f32 array(float32), f64 array(float64), date array(date), date_time array(datetime('Europe/Moscow')), str array(string), fixed_string array(fixed_string(5)), INDEX idx (i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, date, date_time, str, fixed_string) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree() ORDER BY order_key SETTINGS index_granularity = 6;
INSERT INTO bloom_filter_array_types_test SELECT group_array(number) AS order_key, group_array(to_int8(number)) AS i8, group_array(to_int16(number)) AS i16, group_array(to_int32(number)) AS i32, group_array(to_int64(number)) AS i64, group_array(to_uint8(number)) AS u8, group_array(to_uint16(number)) AS u16, group_array(to_uint32(number)) AS u32, group_array(to_uint64(number)) AS u64, group_array(to_float32(number)) AS f32, group_array(to_float64(number)) AS f64, group_array(to_date(number, 'Europe/Moscow')) AS date, group_array(to_datetime(number, 'Europe/Moscow')) AS date_time, group_array(to_string(number)) AS str, group_array(to_fixed_string(to_string(number), 5)) AS fixed_string FROM (SELECT number FROM system.numbers LIMIT 15);
INSERT INTO bloom_filter_array_types_test SELECT group_array(number) AS order_key, group_array(to_int8(number)) AS i8, group_array(to_int16(number)) AS i16, group_array(to_int32(number)) AS i32, group_array(to_int64(number)) AS i64, group_array(to_uint8(number)) AS u8, group_array(to_uint16(number)) AS u16, group_array(to_uint32(number)) AS u32, group_array(to_uint64(number)) AS u64, group_array(to_float32(number)) AS f32, group_array(to_float64(number)) AS f64, group_array(to_date(number, 'Europe/Moscow')) AS date, group_array(to_datetime(number, 'Europe/Moscow')) AS date_time, group_array(to_string(number)) AS str, group_array(to_fixed_string(to_string(number), 5)) AS fixed_string FROM (SELECT number FROM system.numbers WHERE number >= 5 LIMIT 15);
INSERT INTO bloom_filter_array_types_test SELECT group_array(number) AS order_key, group_array(to_int8(number)) AS i8, group_array(to_int16(number)) AS i16, group_array(to_int32(number)) AS i32, group_array(to_int64(number)) AS i64, group_array(to_uint8(number)) AS u8, group_array(to_uint16(number)) AS u16, group_array(to_uint32(number)) AS u32, group_array(to_uint64(number)) AS u64, group_array(to_float32(number)) AS f32, group_array(to_float64(number)) AS f64, group_array(to_date(number, 'Europe/Moscow')) AS date, group_array(to_datetime(number, 'Europe/Moscow')) AS date_time, group_array(to_string(number)) AS str, group_array(to_fixed_string(to_string(number), 5)) AS fixed_string FROM (SELECT number FROM system.numbers WHERE number >= 10 LIMIT 15);

SELECT count() FROM bloom_filter_array_types_test WHERE has(i8, 1);
SELECT count() FROM bloom_filter_array_types_test WHERE has(i16, 1);
SELECT count() FROM bloom_filter_array_types_test WHERE has(i32, 1);
SELECT count() FROM bloom_filter_array_types_test WHERE has(i64, 1);
SELECT count() FROM bloom_filter_array_types_test WHERE has(u8, 1);
SELECT count() FROM bloom_filter_array_types_test WHERE has(u16, 1);
SELECT count() FROM bloom_filter_array_types_test WHERE has(u32, 1);
SELECT count() FROM bloom_filter_array_types_test WHERE has(u64, 1);
SELECT count() FROM bloom_filter_array_types_test WHERE has(f32, 1);
SELECT count() FROM bloom_filter_array_types_test WHERE has(f64, 1);
SELECT count() FROM bloom_filter_array_types_test WHERE has(date, to_date('1970-01-02'));
SELECT count() FROM bloom_filter_array_types_test WHERE has(date_time, to_datetime('1970-01-01 03:00:01', 'Europe/Moscow'));
SELECT count() FROM bloom_filter_array_types_test WHERE has(str, '1');
SELECT count() FROM bloom_filter_array_types_test WHERE has(fixed_string, to_fixed_string('1', 5));

SELECT count() FROM bloom_filter_array_types_test WHERE has(i8, 5);
SELECT count() FROM bloom_filter_array_types_test WHERE has(i16, 5);
SELECT count() FROM bloom_filter_array_types_test WHERE has(i32, 5);
SELECT count() FROM bloom_filter_array_types_test WHERE has(i64, 5);
SELECT count() FROM bloom_filter_array_types_test WHERE has(u8, 5);
SELECT count() FROM bloom_filter_array_types_test WHERE has(u16, 5);
SELECT count() FROM bloom_filter_array_types_test WHERE has(u32, 5);
SELECT count() FROM bloom_filter_array_types_test WHERE has(u64, 5);
SELECT count() FROM bloom_filter_array_types_test WHERE has(f32, 5);
SELECT count() FROM bloom_filter_array_types_test WHERE has(f64, 5);
SELECT count() FROM bloom_filter_array_types_test WHERE has(date, to_date('1970-01-06'));
SELECT count() FROM bloom_filter_array_types_test WHERE has(date_time, to_datetime('1970-01-01 03:00:05', 'Europe/Moscow'));
SELECT count() FROM bloom_filter_array_types_test WHERE has(str, '5');
SELECT count() FROM bloom_filter_array_types_test WHERE has(fixed_string, to_fixed_string('5', 5));

SELECT count() FROM bloom_filter_array_types_test WHERE has(i8, 10);
SELECT count() FROM bloom_filter_array_types_test WHERE has(i16, 10);
SELECT count() FROM bloom_filter_array_types_test WHERE has(i32, 10);
SELECT count() FROM bloom_filter_array_types_test WHERE has(i64, 10);
SELECT count() FROM bloom_filter_array_types_test WHERE has(u8, 10);
SELECT count() FROM bloom_filter_array_types_test WHERE has(u16, 10);
SELECT count() FROM bloom_filter_array_types_test WHERE has(u32, 10);
SELECT count() FROM bloom_filter_array_types_test WHERE has(u64, 10);
SELECT count() FROM bloom_filter_array_types_test WHERE has(f32, 10);
SELECT count() FROM bloom_filter_array_types_test WHERE has(f64, 10);
SELECT count() FROM bloom_filter_array_types_test WHERE has(date, to_date('1970-01-11'));
SELECT count() FROM bloom_filter_array_types_test WHERE has(date_time, to_datetime('1970-01-01 03:00:10', 'Europe/Moscow'));
SELECT count() FROM bloom_filter_array_types_test WHERE has(str, '10');
SELECT count() FROM bloom_filter_array_types_test WHERE has(fixed_string, to_fixed_string('10', 5));

DROP STREAM IF EXISTS bloom_filter_array_types_test;

DROP STREAM IF EXISTS bloom_filter_null_types_test;

create stream bloom_filter_null_types_test (order_key uint64, i8 nullable(int8), i16 nullable(int16), i32 nullable(int32), i64 nullable(int64), u8 nullable(uint8), u16 nullable(uint16), u32 nullable(uint32), u64 nullable(uint64), f32 nullable(float32), f64 nullable(float64), date nullable(date), date_time nullable(datetime('Europe/Moscow')), str nullable(string), fixed_string nullable(fixed_string(5)), INDEX idx (i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, date, date_time, str, fixed_string) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree() ORDER BY order_key SETTINGS index_granularity = 6;
INSERT INTO bloom_filter_null_types_test SELECT number AS order_key, to_int8(number) AS i8, to_int16(number) AS i16, to_int32(number) AS i32, to_int64(number) AS i64, to_uint8(number) AS u8, to_uint16(number) AS u16, to_uint32(number) AS u32, to_uint64(number) AS u64, to_float32(number) AS f32, to_float64(number) AS f64, to_date(number, 'Europe/Moscow') AS date, to_datetime(number, 'Europe/Moscow') AS date_time, to_string(number) AS str, to_fixed_string(to_string(number), 5) AS fixed_string FROM system.numbers LIMIT 100;
INSERT INTO bloom_filter_null_types_test SELECT 0 AS order_key, NULL AS i8, NULL AS i16, NULL AS i32, NULL AS i64, NULL AS u8, NULL AS u16, NULL AS u32, NULL AS u64, NULL AS f32, NULL AS f64, NULL AS date, NULL AS date_time, NULL AS str, NULL AS fixed_string;

SELECT count() FROM bloom_filter_null_types_test WHERE i8 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_null_types_test WHERE i16 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_null_types_test WHERE i32 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_null_types_test WHERE i64 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_null_types_test WHERE u8 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_null_types_test WHERE u16 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_null_types_test WHERE u32 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_null_types_test WHERE u64 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_null_types_test WHERE f32 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_null_types_test WHERE f64 = 1 SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_null_types_test WHERE date = '1970-01-02' SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_null_types_test WHERE date_time = to_datetime('1970-01-01 03:00:01', 'Europe/Moscow') SETTINGS max_rows_to_read = 6;
SELECT count() FROM bloom_filter_null_types_test WHERE str = '1' SETTINGS max_rows_to_read = 12;
SELECT count() FROM bloom_filter_null_types_test WHERE fixed_string = to_fixed_string('1', 5) SETTINGS max_rows_to_read = 12;

SELECT count() FROM bloom_filter_null_types_test WHERE is_null(i8);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(i16);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(i32);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(i64);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(u8);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(u16);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(u32);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(u64);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(f32);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(f64);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(date);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(date_time);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(str);
SELECT count() FROM bloom_filter_null_types_test WHERE is_null(fixed_string);

SELECT count() FROM bloom_filter_null_types_test WHERE str IN ( SELECT str FROM bloom_filter_null_types_test);

DROP STREAM IF EXISTS bloom_filter_null_types_test;

DROP STREAM IF EXISTS bloom_filter_lc_null_types_test;

create stream bloom_filter_lc_null_types_test (order_key uint64, str low_cardinality(nullable(string)), fixed_string low_cardinality(nullable(fixed_string(5))), INDEX idx (str, fixed_string) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree() ORDER BY order_key SETTINGS index_granularity = 6;
INSERT INTO bloom_filter_lc_null_types_test SELECT number AS order_key, to_string(number) AS str, to_fixed_string(to_string(number), 5) AS fixed_string FROM system.numbers LIMIT 100;
INSERT INTO bloom_filter_lc_null_types_test SELECT 0 AS order_key, NULL AS str, NULL AS fixed_string;

SELECT count() FROM bloom_filter_lc_null_types_test WHERE str = '1' SETTINGS max_rows_to_read = 12;
SELECT count() FROM bloom_filter_lc_null_types_test WHERE fixed_string = to_fixed_string('1', 5) SETTINGS max_rows_to_read = 12;

SELECT count() FROM bloom_filter_lc_null_types_test WHERE is_null(str);
SELECT count() FROM bloom_filter_lc_null_types_test WHERE is_null(fixed_string);

SELECT count() FROM bloom_filter_lc_null_types_test WHERE str IN ( SELECT str FROM bloom_filter_lc_null_types_test);

DROP STREAM IF EXISTS bloom_filter_lc_null_types_test;

DROP STREAM IF EXISTS bloom_filter_array_lc_null_types_test;

create stream bloom_filter_array_lc_null_types_test (
    order_key   array(low_cardinality(nullable(uint64))),

    i8 array(low_cardinality(nullable(int8))),
    i16 array(low_cardinality(nullable(int16))),
    i32 array(low_cardinality(nullable(int32))),
    i64 array(low_cardinality(nullable(int64))),
    u8 array(low_cardinality(nullable(uint8))),
    u16 array(low_cardinality(nullable(uint16))),
    u32 array(low_cardinality(nullable(uint32))),
    u64 array(low_cardinality(nullable(uint64))),
    f32 array(low_cardinality(nullable(float32))),
    f64 array(low_cardinality(nullable(float64))),

    date array(low_cardinality(nullable(date))),
    date_time array(low_cardinality(nullable(datetime('Europe/Moscow')))),

    str array(low_cardinality(nullable(string))),
    fixed_string array(low_cardinality(nullable(fixed_string(5)))),
    INDEX idx (i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, date, date_time, str, fixed_string)
    TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree() ORDER BY order_key SETTINGS index_granularity = 6, allow_nullable_key = 1;

INSERT INTO bloom_filter_array_lc_null_types_test
SELECT group_array(number) AS order_key,
    group_array(to_int8(number)) AS i8,
    group_array(to_int16(number)) AS i16,
    group_array(to_int32(number)) AS i32,
    group_array(to_int64(number)) AS i64,
    group_array(to_uint8(number)) AS u8,
    group_array(to_uint16(number)) AS u16,
    group_array(to_uint32(number)) AS u32,
    group_array(to_uint64(number)) AS u64,
    group_array(to_float32(number)) AS f32,
    group_array(to_float64(number)) AS f64,
    group_array(to_date(number, 'Europe/Moscow')) AS date,
    group_array(to_datetime(number, 'Europe/Moscow')) AS date_time,
    group_array(to_string(number)) AS str,
    group_array(to_fixed_string(to_string(number), 5)) AS fixed_string
    FROM (SELECT number FROM system.numbers LIMIT 15);

INSERT INTO bloom_filter_array_lc_null_types_test SELECT group_array(number) AS order_key, group_array(to_int8(number)) AS i8, group_array(to_int16(number)) AS i16, group_array(to_int32(number)) AS i32, group_array(to_int64(number)) AS i64, group_array(to_uint8(number)) AS u8, group_array(to_uint16(number)) AS u16, group_array(to_uint32(number)) AS u32, group_array(to_uint64(number)) AS u64, group_array(to_float32(number)) AS f32, group_array(to_float64(number)) AS f64, group_array(to_date(number, 'Europe/Moscow')) AS date, group_array(to_datetime(number, 'Europe/Moscow')) AS date_time, group_array(to_string(number)) AS str, group_array(to_fixed_string(to_string(number), 5)) AS fixed_string FROM (SELECT number FROM system.numbers WHERE number >= 5 LIMIT 15);
INSERT INTO bloom_filter_array_lc_null_types_test SELECT group_array(number) AS order_key, group_array(to_int8(number)) AS i8, group_array(to_int16(number)) AS i16, group_array(to_int32(number)) AS i32, group_array(to_int64(number)) AS i64, group_array(to_uint8(number)) AS u8, group_array(to_uint16(number)) AS u16, group_array(to_uint32(number)) AS u32, group_array(to_uint64(number)) AS u64, group_array(to_float32(number)) AS f32, group_array(to_float64(number)) AS f64, group_array(to_date(number, 'Europe/Moscow')) AS date, group_array(to_datetime(number, 'Europe/Moscow')) AS date_time, group_array(to_string(number)) AS str, group_array(to_fixed_string(to_string(number), 5)) AS fixed_string FROM (SELECT number FROM system.numbers WHERE number >= 10 LIMIT 15);
INSERT INTO bloom_filter_array_lc_null_types_test SELECT n AS order_key, n AS i8, n AS i16, n AS i32, n AS i64, n AS u8, n AS u16, n AS u32, n AS u64, n AS f32, n AS f64, n AS date, n AS date_time, n AS str, n AS fixed_string FROM (SELECT [NULL] AS n);
INSERT INTO bloom_filter_array_lc_null_types_test SELECT [NULL, n] AS order_key, [NULL, to_int8(n)] AS i8, [NULL, to_int16(n)] AS i16, [NULL, to_int32(n)] AS i32, [NULL, to_int64(n)] AS i64, [NULL, to_uint8(n)] AS u8, [NULL, to_uint16(n)] AS u16, [NULL, to_uint32(n)] AS u32, [NULL, to_uint64(n)] AS u64, [NULL, to_float32(n)] AS f32, [NULL, to_float64(n)] AS f64, [NULL, to_date(n, 'Europe/Moscow')] AS date, [NULL, to_datetime(n, 'Europe/Moscow')] AS date_time, [NULL, to_string(n)] AS str, [NULL, to_fixed_string(to_string(n), 5)] AS fixed_string FROM (SELECT 100 as n);

SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i8, 1);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i16, 1);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i32, 1);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i64, 1);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u8, 1);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u16, 1);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u32, 1);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u64, 1);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(f32, 1);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(f64, 1);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(date, to_date('1970-01-02'));
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(date_time, to_datetime('1970-01-01 03:00:01', 'Europe/Moscow'));
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(str, '1');
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(fixed_string, to_fixed_string('1', 5));

SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i8, 5);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i16, 5);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i32, 5);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i64, 5);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u8, 5);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u16, 5);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u32, 5);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u64, 5);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(f32, 5);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(f64, 5);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(date, to_date('1970-01-06'));
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(date_time, to_datetime('1970-01-01 03:00:05', 'Europe/Moscow'));
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(str, '5');
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(fixed_string, to_fixed_string('5', 5));

SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i8, 10);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i16, 10);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i32, 10);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i64, 10);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u8, 10);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u16, 10);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u32, 10);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u64, 10);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(f32, 10);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(f64, 10);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(date, to_date('1970-01-11'));
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(date_time, to_datetime('1970-01-01 03:00:10', 'Europe/Moscow'));
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(str, '10');
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(fixed_string, to_fixed_string('10', 5));

SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i8, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i16, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i32, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i64, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u8, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u16, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u32, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u64, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(f32, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(f64, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(date, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(date_time, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(str, NULL);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(fixed_string, NULL);

SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i8, 100);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i16, 100);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i32, 100);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(i64, 100);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u8, 100);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u16, 100);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u32, 100);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(u64, 100);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(f32, 100);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(f64, 100);
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(date, to_date('1970-04-11'));
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(date_time, to_datetime('1970-01-01 03:01:40', 'Europe/Moscow'));
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(str, '100');
SELECT count() FROM bloom_filter_array_lc_null_types_test WHERE has(fixed_string, to_fixed_string('100', 5));

DROP STREAM IF EXISTS bloom_filter_array_lc_null_types_test;

DROP STREAM IF EXISTS bloom_filter_array_offsets_lc_str;
create stream bloom_filter_array_offsets_lc_str (order_key int, str array(low_cardinality(string)), INDEX idx str TYPE bloom_filter(1.) GRANULARITY 1024) ENGINE = MergeTree() ORDER BY order_key SETTINGS index_granularity = 1024;
INSERT INTO bloom_filter_array_offsets_lc_str SELECT number AS i, if(i%2, ['value'], []) FROM system.numbers LIMIT 10000;
SELECT count() FROM bloom_filter_array_offsets_lc_str WHERE has(str, 'value');
DROP STREAM IF EXISTS bloom_filter_array_offsets_lc_str;

DROP STREAM IF EXISTS bloom_filter_array_offsets_str;
create stream bloom_filter_array_offsets_str (order_key int, str array(string), INDEX idx str TYPE bloom_filter(1.) GRANULARITY 1024) ENGINE = MergeTree() ORDER BY order_key SETTINGS index_granularity = 1024;
INSERT INTO bloom_filter_array_offsets_str SELECT number AS i, if(i%2, ['value'], []) FROM system.numbers LIMIT 10000;
SELECT count() FROM bloom_filter_array_offsets_str WHERE has(str, 'value');
DROP STREAM IF EXISTS bloom_filter_array_offsets_str;

DROP STREAM IF EXISTS bloom_filter_array_offsets_i;
create stream bloom_filter_array_offsets_i (order_key int, i array(int), INDEX idx i TYPE bloom_filter(1.) GRANULARITY 1024) ENGINE = MergeTree() ORDER BY order_key SETTINGS index_granularity = 1024;
INSERT INTO bloom_filter_array_offsets_i SELECT number AS i, if(i%2, [99999], []) FROM system.numbers LIMIT 10000;
SELECT count() FROM bloom_filter_array_offsets_i WHERE has(i, 99999);
DROP STREAM IF EXISTS bloom_filter_array_offsets_i;

DROP STREAM IF EXISTS test_bf_indexOf;
create stream test_bf_indexOf ( `id` int, `ary` array(low_cardinality(nullable(string))), INDEX idx_ary ary TYPE bloom_filter(0.01) GRANULARITY 1) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_bf_indexOf VALUES (1, ['value1', 'value2']);
INSERT INTO test_bf_indexOf VALUES (2, ['value3']);

SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') = 0 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') = 1 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value2') = 0 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value2') = 2 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value3') = 0 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value3') = 1 ORDER BY id FORMAT TSV;

SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') != 0 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') != 1 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value2') != 0 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value2') != 2 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value3') != 0 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value3') != 1 ORDER BY id FORMAT TSV;

SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') = 2 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value3') = 2 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') = 1 OR index_of(ary, 'value3') = 1 ORDER BY id FORMAT TSV;

SELECT id FROM test_bf_indexOf WHERE not(index_of(ary, 'value1')) ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE not(index_of(ary, 'value1') == 0) ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE not(index_of(ary, 'value1') == 1) ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE not(index_of(ary, 'value1') == 2) ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') in (0) ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') in (1) ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') in (2) ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') not in (0) ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') not in (1) ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') not in (2) ORDER BY id FORMAT TSV;

SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') > 0 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE 0 < index_of(ary, 'value1') ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') >= 0 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE 0 <= index_of(ary, 'value1') ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') > 1 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE 1 < index_of(ary, 'value1') ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') >= 1 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE 1 <= index_of(ary, 'value1') ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') >= 2 ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE 2 <= index_of(ary, 'value1') ORDER BY id FORMAT TSV;

SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') = to_decimal32(0, 2) ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE to_decimal128(0, 2) = index_of(ary, 'value1') ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') = '0' ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE '0' = index_of(ary, 'value1') ORDER BY id FORMAT TSV;

SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') > to_decimal32(0, 2) ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') < to_decimal128(1, 2) ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') > '0' ORDER BY id FORMAT TSV;
SELECT id FROM test_bf_indexOf WHERE index_of(ary, 'value1') < '1' ORDER BY id FORMAT TSV;

SELECT id, ary[index_of(ary, 'value1')] FROM test_bf_indexOf WHERE ary[index_of(ary, 'value1')] = 'value1' ORDER BY id FORMAT TSV;
SELECT id, ary[index_of(ary, 'value2')] FROM test_bf_indexOf WHERE ary[index_of(ary, 'value2')] = 'value2' ORDER BY id FORMAT TSV;
SELECT id, ary[index_of(ary, 'value3')] FROM test_bf_indexOf WHERE ary[index_of(ary, 'value3')] = 'value3' ORDER BY id FORMAT TSV;

DROP STREAM IF EXISTS test_bf_indexOf;
