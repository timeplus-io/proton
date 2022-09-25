SELECT accurateCastOrDefault(-1, 'uint8'), accurateCastOrDefault(5, 'uint8');
SELECT accurateCastOrDefault(5, 'uint8');
SELECT accurateCastOrDefault(257, 'uint8'),  accurateCastOrDefault(257, 'uint8', 5);
SELECT accurateCastOrDefault(-1, 'uint16'),  accurateCastOrDefault(-1, 'uint16', to_uint16(5));
SELECT accurateCastOrDefault(5, 'uint16');
SELECT accurateCastOrDefault(65536, 'uint16'),  accurateCastOrDefault(65536, 'uint16', to_uint16(5));
SELECT accurateCastOrDefault(-1, 'uint32'),  accurateCastOrDefault(-1, 'uint32', to_uint32(5));
SELECT accurateCastOrDefault(5, 'uint32');
SELECT accurateCastOrDefault(4294967296, 'uint32'),  accurateCastOrDefault(4294967296, 'uint32', to_uint32(5));
SELECT accurateCastOrDefault(-1, 'uint64'), accurateCastOrDefault(-1, 'uint64', to_uint64(5));
SELECT accurateCastOrDefault(5, 'uint64');
SELECT accurateCastOrDefault(-1, 'UInt256'), accurateCastOrDefault(-1, 'UInt256', toUInt256(5));
SELECT accurateCastOrDefault(5, 'UInt256');
SELECT accurateCastOrDefault(-129, 'int8'), accurateCastOrDefault(-129, 'int8', to_int8(5));
SELECT accurateCastOrDefault(5, 'int8');
SELECT accurateCastOrDefault(128, 'int8'),  accurateCastOrDefault(128, 'int8', to_int8(5));

SELECT accurateCastOrDefault(10, 'Decimal32(9)'), accurateCastOrDefault(10, 'Decimal32(9)', to_decimal32(2, 9));
SELECT accurateCastOrDefault(1, 'Decimal32(9)');
SELECT accurateCastOrDefault(-10, 'Decimal32(9)'), accurateCastOrDefault(-10, 'Decimal32(9)', to_decimal32(2, 9));

SELECT accurateCastOrDefault('123', 'fixed_string(2)'), accurateCastOrDefault('123', 'fixed_string(2)', cast('12', 'fixed_string(2)'));

SELECT accurateCastOrDefault(inf, 'int64'), accurateCastOrDefault(inf, 'int64', to_int64(5));
SELECT accurateCastOrDefault(inf, 'Int128'), accurateCastOrDefault(inf, 'Int128', to_int128(5));
SELECT accurateCastOrDefault(inf, 'Int256'), accurateCastOrDefault(inf, 'Int256', toInt256(5));
SELECT accurateCastOrDefault(nan, 'int64'), accurateCastOrDefault(nan, 'int64', to_int64(5));
SELECT accurateCastOrDefault(nan, 'Int128'), accurateCastOrDefault(nan, 'Int128', to_int128(5));
SELECT accurateCastOrDefault(nan, 'Int256'), accurateCastOrDefault(nan, 'Int256', toInt256(5));

SELECT accurateCastOrDefault(inf, 'uint64'), accurateCastOrDefault(inf, 'uint64', to_uint64(5));
SELECT accurateCastOrDefault(inf, 'UInt256'), accurateCastOrDefault(inf, 'UInt256', toUInt256(5));
SELECT accurateCastOrDefault(nan, 'uint64'), accurateCastOrDefault(nan, 'uint64', to_uint64(5));
SELECT accurateCastOrDefault(nan, 'UInt256'), accurateCastOrDefault(nan, 'UInt256', toUInt256(5));

SELECT accurateCastOrDefault(number + 127, 'int8') AS x, accurateCastOrDefault(number + 127, 'int8', to_int8(5)) AS x_with_default FROM numbers (2) ORDER BY number;
