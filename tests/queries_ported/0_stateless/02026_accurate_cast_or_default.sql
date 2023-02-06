SELECT accurate_cast_or_default(-1, 'uint8'), accurate_cast_or_default(5, 'uint8');
SELECT accurate_cast_or_default(5, 'uint8');
SELECT accurate_cast_or_default(257, 'uint8'),  accurate_cast_or_default(257, 'uint8', 5);
SELECT accurate_cast_or_default(-1, 'uint16'),  accurate_cast_or_default(-1, 'uint16', to_uint16(5));
SELECT accurate_cast_or_default(5, 'uint16');
SELECT accurate_cast_or_default(65536, 'uint16'),  accurate_cast_or_default(65536, 'uint16', to_uint16(5));
SELECT accurate_cast_or_default(-1, 'uint32'),  accurate_cast_or_default(-1, 'uint32', to_uint32(5));
SELECT accurate_cast_or_default(5, 'uint32');
SELECT accurate_cast_or_default(4294967296, 'uint32'),  accurate_cast_or_default(4294967296, 'uint32', to_uint32(5));
SELECT accurate_cast_or_default(-1, 'uint64'), accurate_cast_or_default(-1, 'uint64', to_uint64(5));
SELECT accurate_cast_or_default(5, 'uint64');
SELECT accurate_cast_or_default(-1, 'uint256'), accurate_cast_or_default(-1, 'uint256', to_uint256(5));
SELECT accurate_cast_or_default(5, 'uint256');
SELECT accurate_cast_or_default(-129, 'int8'), accurate_cast_or_default(-129, 'int8', to_int8(5));
SELECT accurate_cast_or_default(5, 'int8');
SELECT accurate_cast_or_default(128, 'int8'),  accurate_cast_or_default(128, 'int8', to_int8(5));

SELECT accurate_cast_or_default(10, 'decimal32(9)'), accurate_cast_or_default(10, 'decimal32(9)', to_decimal32(2, 9));
SELECT accurate_cast_or_default(1, 'decimal32(9)');
SELECT accurate_cast_or_default(-10, 'decimal32(9)'), accurate_cast_or_default(-10, 'decimal32(9)', to_decimal32(2, 9));

SELECT accurate_cast_or_default('123', 'fixed_string(2)'), accurate_cast_or_default('123', 'fixed_string(2)', cast('12', 'fixed_string(2)'));

SELECT accurate_cast_or_default(inf, 'int64'), accurate_cast_or_default(inf, 'int64', to_int64(5));
SELECT accurate_cast_or_default(inf, 'int128'), accurate_cast_or_default(inf, 'int128', to_int128(5));
SELECT accurate_cast_or_default(inf, 'int256'), accurate_cast_or_default(inf, 'int256', to_int256(5));
SELECT accurate_cast_or_default(nan, 'int64'), accurate_cast_or_default(nan, 'int64', to_int64(5));
SELECT accurate_cast_or_default(nan, 'int128'), accurate_cast_or_default(nan, 'int128', to_int128(5));
SELECT accurate_cast_or_default(nan, 'int256'), accurate_cast_or_default(nan, 'int256', to_int256(5));

SELECT accurate_cast_or_default(inf, 'uint64'), accurate_cast_or_default(inf, 'uint64', to_uint64(5));
SELECT accurate_cast_or_default(inf, 'uint256'), accurate_cast_or_default(inf, 'uint256', to_uint256(5));
SELECT accurate_cast_or_default(nan, 'uint64'), accurate_cast_or_default(nan, 'uint64', to_uint64(5));
SELECT accurate_cast_or_default(nan, 'uint256'), accurate_cast_or_default(nan, 'uint256', to_uint256(5));

SELECT accurate_cast_or_default(number + 127, 'int8') AS x, accurate_cast_or_default(number + 127, 'int8', to_int8(5)) AS x_with_default FROM numbers (2) ORDER BY number;
