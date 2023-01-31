SET allow_experimental_bigint_types=1;

CREATE STREAM IF NOT EXISTS test_01035_avg (
    i8 int8         DEFAULT i64,
    i16 int16       DEFAULT i64,
    i32 int32       DEFAULT i64,
    i64 int64       DEFAULT if(u64 % 2 = 0, to_int64(u64), to_int64(-u64)),
    i128 int128     DEFAULT i64,
    i256 int256     DEFAULT i64,

    u8 uint8        DEFAULT u64,
    u16 uint16      DEFAULT u64,
    u32 uint32      DEFAULT u64,
    u64 uint64,
    u128 uint128    DEFAULT u64,
    u256 uint256    DEFAULT u64,

    f32 float32     DEFAULT u64,
    f64 float64     DEFAULT u64,

    d32 decimal32(4)    DEFAULT to_decimal32(i32 / 1000, 4),
    d64 decimal64(18)   DEFAULT to_decimal64(u64 / 1000000, 8),
    d128 decimal128(20) DEFAULT to_decimal128(i128 / 100000, 20),
    d256 decimal256(40) DEFAULT to_decimal256(i256 / 100000, 40)
) ENGINE = MergeTree() ORDER BY i64;

SELECT avg(i8), avg(i16), avg(i32), avg(i64), avg(i128), avg(i256),
       avg(u8), avg(u16), avg(u32), avg(u64), avg(u128), avg(u256),
       avg(f32), avg(f64),
       avg(d32), avg(d64), avg(d128), avg(d256) FROM test_01035_avg;

INSERT INTO test_01035_avg (u64) SELECT number FROM system.numbers LIMIT 1000000;

SELECT avg(i8), avg(i16), avg(i32), avg(i64), avg(i128), avg(i256),
       avg(u8), avg(u16), avg(u32), avg(u64), avg(u128), avg(u256),
       avg(f32), avg(f64),
       avg(d32), avg(d64), avg(d128), avg(d256) FROM test_01035_avg;

SELECT avg(i8 * i16) FROM test_01035_avg;
SELECT avg(f32 + f64) FROM test_01035_avg;
SELECT avg(d128 - d64) FROM test_01035_avg;

DROP STREAM IF EXISTS test_01035_avg;

-- Checks that the internal SUM does not overflow int8
SELECT avg(key), avgIf(key, key > 0), avg(key2), avgIf(key2, key2 > 0), avg(key3), avgIf(key3, key3 > 0)
FROM
(
     SELECT 1::int8 as key, Null::nullable(int8) AS key2, 1::nullable(int8) as key3
     FROM numbers(100000)
)
