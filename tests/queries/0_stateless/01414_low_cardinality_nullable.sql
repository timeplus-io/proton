DROP STREAM IF EXISTS lc_nullable;

create stream lc_nullable (
    order_key   array(low_cardinality(nullable(uint64))),

    i8  array(low_cardinality(nullable(int8))),
    i16 array(low_cardinality(nullable(int16))),
    i32 array(low_cardinality(nullable(int32))),
    i64 array(low_cardinality(nullable(int64))),
    u8  array(low_cardinality(nullable(uint8))),
    u16 array(low_cardinality(nullable(uint16))),
    u32 array(low_cardinality(nullable(uint32))),
    u64 array(low_cardinality(nullable(uint64))),
    f32 array(low_cardinality(nullable(float32))),
    f64 array(low_cardinality(nullable(float64))),

    date array(low_cardinality(nullable(date))),
    date_time array(low_cardinality(nullable(datetime('Europe/Moscow')))),

    str array(low_cardinality(nullable(string))),
    fixed_string array(low_cardinality(nullable(fixed_string(5))))
) ENGINE = MergeTree() ORDER BY order_key SETTINGS allow_nullable_key = 1;

INSERT INTO lc_nullable SELECT
    group_array(number) AS order_key,
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

INSERT INTO lc_nullable SELECT
    group_array(num) AS order_key,
    group_array(to_int8(num)) AS i8,
    group_array(to_int16(num)) AS i16,
    group_array(to_int32(num)) AS i32,
    group_array(to_int64(num)) AS i64,
    group_array(to_uint8(num)) AS u8,
    group_array(to_uint16(num)) AS u16,
    group_array(to_uint32(num)) AS u32,
    group_array(to_uint64(num)) AS u64,
    group_array(to_float32(num)) AS f32,
    group_array(to_float64(num)) AS f64,
    group_array(to_date(num, 'Europe/Moscow')) AS date,
    group_array(to_datetime(num, 'Europe/Moscow')) AS date_time,
    group_array(to_string(num)) AS str,
    group_array(to_fixed_string(to_string(num), 5)) AS fixed_string
    FROM (SELECT negate(number) as num FROM system.numbers LIMIT 15);

INSERT INTO lc_nullable SELECT
    group_array(number) AS order_key,
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
    FROM (SELECT number FROM system.numbers WHERE number >= 5 LIMIT 15);

INSERT INTO lc_nullable SELECT
    group_array(number) AS order_key,
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
    FROM (SELECT number FROM system.numbers WHERE number >= 10 LIMIT 15);

INSERT INTO lc_nullable SELECT
    n AS order_key,
    n AS i8,
    n AS i16,
    n AS i32,
    n AS i64,
    n AS u8,
    n AS u16,
    n AS u32,
    n AS u64,
    n AS f32,
    n AS f64,
    n AS date,
    n AS date_time,
    n AS str,
    n AS fixed_string
    FROM (SELECT [NULL] AS n);

INSERT INTO lc_nullable SELECT
    [NULL, n] AS order_key,
    [NULL, to_int8(n)] AS i8,
    [NULL, to_int16(n)] AS i16,
    [NULL, to_int32(n)] AS i32,
    [NULL, to_int64(n)] AS i64,
    [NULL, to_uint8(n)] AS u8,
    [NULL, to_uint16(n)] AS u16,
    [NULL, to_uint32(n)] AS u32,
    [NULL, to_uint64(n)] AS u64,
    [NULL, to_float32(n)] AS f32,
    [NULL, to_float64(n)] AS f64,
    [NULL, to_date(n, 'Europe/Moscow')] AS date,
    [NULL, to_datetime(n, 'Europe/Moscow')] AS date_time,
    [NULL, to_string(n)] AS str,
    [NULL, to_fixed_string(to_string(n), 5)] AS fixed_string
    FROM (SELECT 100 as n);

SELECT count() FROM lc_nullable WHERE has(i8, 1);
SELECT count() FROM lc_nullable WHERE has(i16, 1);
SELECT count() FROM lc_nullable WHERE has(i32, 1);
SELECT count() FROM lc_nullable WHERE has(i64, 1);
SELECT count() FROM lc_nullable WHERE has(u8, 1);
SELECT count() FROM lc_nullable WHERE has(u16, 1);
SELECT count() FROM lc_nullable WHERE has(u32, 1);
SELECT count() FROM lc_nullable WHERE has(u64, 1);
SELECT count() FROM lc_nullable WHERE has(f32, 1);
SELECT count() FROM lc_nullable WHERE has(f64, 1);
SELECT count() FROM lc_nullable WHERE has(date, to_date('1970-01-02'));
SELECT count() FROM lc_nullable WHERE has(date_time, to_datetime('1970-01-01 03:00:01', 'Europe/Moscow'));
SELECT count() FROM lc_nullable WHERE has(str, '1');
SELECT count() FROM lc_nullable WHERE has(fixed_string, to_fixed_string('1', 5));

SELECT count() FROM lc_nullable WHERE has(i8,  -1);
SELECT count() FROM lc_nullable WHERE has(i16, -1);
SELECT count() FROM lc_nullable WHERE has(i32, -1);
SELECT count() FROM lc_nullable WHERE has(i64, -1);
SELECT count() FROM lc_nullable WHERE has(u8,  -1);
SELECT count() FROM lc_nullable WHERE has(u16, -1);
SELECT count() FROM lc_nullable WHERE has(u32, -1);
SELECT count() FROM lc_nullable WHERE has(u64, -1);
SELECT count() FROM lc_nullable WHERE has(f32, -1);
SELECT count() FROM lc_nullable WHERE has(f64, -1);
SELECT count() FROM lc_nullable WHERE has(str, '-1');
SELECT count() FROM lc_nullable WHERE has(fixed_string, to_fixed_string('-1', 5));

SELECT count() FROM lc_nullable WHERE has(i8, 5);
SELECT count() FROM lc_nullable WHERE has(i16, 5);
SELECT count() FROM lc_nullable WHERE has(i32, 5);
SELECT count() FROM lc_nullable WHERE has(i64, 5);
SELECT count() FROM lc_nullable WHERE has(u8, 5);
SELECT count() FROM lc_nullable WHERE has(u16, 5);
SELECT count() FROM lc_nullable WHERE has(u32, 5);
SELECT count() FROM lc_nullable WHERE has(u64, 5);
SELECT count() FROM lc_nullable WHERE has(f32, 5);
SELECT count() FROM lc_nullable WHERE has(f64, 5);
SELECT count() FROM lc_nullable WHERE has(date, to_date('1970-01-06'));
SELECT count() FROM lc_nullable WHERE has(date_time, to_datetime('1970-01-01 03:00:05', 'Europe/Moscow'));
SELECT count() FROM lc_nullable WHERE has(str, '5');
SELECT count() FROM lc_nullable WHERE has(fixed_string, to_fixed_string('5', 5));

SELECT count() FROM lc_nullable WHERE has(i8, 10);
SELECT count() FROM lc_nullable WHERE has(i16, 10);
SELECT count() FROM lc_nullable WHERE has(i32, 10);
SELECT count() FROM lc_nullable WHERE has(i64, 10);
SELECT count() FROM lc_nullable WHERE has(u8, 10);
SELECT count() FROM lc_nullable WHERE has(u16, 10);
SELECT count() FROM lc_nullable WHERE has(u32, 10);
SELECT count() FROM lc_nullable WHERE has(u64, 10);
SELECT count() FROM lc_nullable WHERE has(f32, 10);
SELECT count() FROM lc_nullable WHERE has(f64, 10);
SELECT count() FROM lc_nullable WHERE has(date, to_date('1970-01-11'));
SELECT count() FROM lc_nullable WHERE has(date_time, to_datetime('1970-01-01 03:00:10', 'Europe/Moscow'));
SELECT count() FROM lc_nullable WHERE has(str, '10');
SELECT count() FROM lc_nullable WHERE has(fixed_string, to_fixed_string('10', 5));

SELECT count() FROM lc_nullable WHERE has(i8, NULL);
SELECT count() FROM lc_nullable WHERE has(i16, NULL);
SELECT count() FROM lc_nullable WHERE has(i32, NULL);
SELECT count() FROM lc_nullable WHERE has(i64, NULL);
SELECT count() FROM lc_nullable WHERE has(u8, NULL);
SELECT count() FROM lc_nullable WHERE has(u16, NULL);
SELECT count() FROM lc_nullable WHERE has(u32, NULL);
SELECT count() FROM lc_nullable WHERE has(u64, NULL);
SELECT count() FROM lc_nullable WHERE has(f32, NULL);
SELECT count() FROM lc_nullable WHERE has(f64, NULL);
SELECT count() FROM lc_nullable WHERE has(date, NULL);
SELECT count() FROM lc_nullable WHERE has(date_time, NULL);
SELECT count() FROM lc_nullable WHERE has(str, NULL);
SELECT count() FROM lc_nullable WHERE has(fixed_string, NULL);

SELECT count() FROM lc_nullable WHERE has(i8, 100);
SELECT count() FROM lc_nullable WHERE has(i16, 100);
SELECT count() FROM lc_nullable WHERE has(i32, 100);
SELECT count() FROM lc_nullable WHERE has(i64, 100);
SELECT count() FROM lc_nullable WHERE has(u8, 100);
SELECT count() FROM lc_nullable WHERE has(u16, 100);
SELECT count() FROM lc_nullable WHERE has(u32, 100);
SELECT count() FROM lc_nullable WHERE has(u64, 100);
SELECT count() FROM lc_nullable WHERE has(f32, 100);
SELECT count() FROM lc_nullable WHERE has(f64, 100);
SELECT count() FROM lc_nullable WHERE has(date, to_date('1970-04-11'));
SELECT count() FROM lc_nullable WHERE has(date_time, to_datetime('1970-01-01 03:01:40', 'Europe/Moscow'));
SELECT count() FROM lc_nullable WHERE has(str, '100');
SELECT count() FROM lc_nullable WHERE has(fixed_string, to_fixed_string('100', 5));

SELECT count() FROM lc_nullable WHERE has(date, to_date(has(u64, 1), '1970-01\002'));

DROP STREAM IF EXISTS lc_nullable;
