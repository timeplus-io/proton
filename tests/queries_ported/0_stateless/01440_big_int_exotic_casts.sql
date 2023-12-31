SELECT to_uint32(number * number) * number  as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_uint64(number * number) * number  as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_uint256(number * number) * number as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_int32(number * number) * number   as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_int64(number * number) * number   as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_int128(number * number) * number  as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_int256(number * number) * number  as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_float32(number * number) * number as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_float64(number * number) * number as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;

SELECT to_uint32(number * number) * -1  as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_uint64(number * number) * -1  as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_uint256(number * number) * -1 as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_int32(number * number) * -1   as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_int64(number * number) * -1   as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_int128(number * number) * -1  as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_int256(number * number) * -1  as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_float32(number * number) * -1 as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT to_float64(number * number) * -1 as y, to_decimal32(y, 1), to_decimal64(y, 2), to_decimal128(y, 6), to_decimal256(y, 7) FROM numbers_mt(10) ORDER BY number;

SELECT to_uint32(number * -1) * number  as y, to_int128(y), to_int256(y), to_uint256(y) FROM numbers_mt(10) ORDER BY number;
SELECT to_uint64(number * -1) * number  as y, to_int128(y), to_int256(y), to_uint256(y) FROM numbers_mt(10) ORDER BY number;
SELECT to_uint256(number * -1) * number as y, to_int128(y), to_int256(y), to_uint256(y) FROM numbers_mt(10) ORDER BY number;
SELECT to_int32(number * -1) * number   as y, to_int128(y), to_int256(y), to_uint256(y) FROM numbers_mt(10) ORDER BY number;
SELECT to_int64(number * -1) * number   as y, to_int128(y), to_int256(y), to_uint256(y) FROM numbers_mt(10) ORDER BY number;
SELECT to_int128(number * -1) * number  as y, to_int128(y), to_int256(y), to_uint256(y) FROM numbers_mt(10) ORDER BY number;
SELECT to_int256(number * -1) * number  as y, to_int128(y), to_int256(y), to_uint256(y) FROM numbers_mt(10) ORDER BY number;
SELECT to_float32(number * -1) * number as y, to_int128(y), to_int256(y), to_uint256(y) FROM numbers_mt(10) ORDER BY number;
SELECT to_float64(number * -1) * number as y, to_int128(y), to_int256(y), to_uint256(y) FROM numbers_mt(10) ORDER BY number;

SELECT number as y, to_int128(number) - y, to_int256(number) - y, to_uint256(number) - y FROM numbers_mt(10) ORDER BY number;
SELECT -number as y, to_int128(number) + y, to_int256(number) + y, to_uint256(number) + y FROM numbers_mt(10) ORDER BY number;


SET allow_experimental_bigint_types = 1;

DROP STREAM IF EXISTS t;
CREATE STREAM t (x uint64, i256 int256, u256 uint256, d256 decimal256(2)) ENGINE = Memory;

INSERT INTO t SELECT number * number * number AS x, x AS i256, x AS u256, x AS d256 FROM numbers(10000);

SELECT sum(x), sum(i256), sum(u256), sum(d256) FROM t;

INSERT INTO t SELECT -number * number * number AS x, x AS i256, x AS u256, x AS d256 FROM numbers(10000);

SELECT sum(x), sum(i256), sum(u256), sum(d256) FROM t;

DROP STREAM t;
