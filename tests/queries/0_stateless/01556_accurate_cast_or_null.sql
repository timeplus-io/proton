SELECT accurateCastOrNull(-1, 'uint8');
SELECT accurateCastOrNull(5, 'uint8');
SELECT accurateCastOrNull(257, 'uint8');
SELECT accurateCastOrNull(-1, 'uint16');
SELECT accurateCastOrNull(5, 'uint16');
SELECT accurateCastOrNull(65536, 'uint16');
SELECT accurateCastOrNull(-1, 'uint32');
SELECT accurateCastOrNull(5, 'uint32');
SELECT accurateCastOrNull(4294967296, 'uint32');
SELECT accurateCastOrNull(-1, 'uint64');
SELECT accurateCastOrNull(5, 'uint64');
SELECT accurateCastOrNull(-1, 'UInt256');
SELECT accurateCastOrNull(5, 'UInt256');

SELECT accurateCastOrNull(-129, 'int8');
SELECT accurateCastOrNull(5, 'int8');
SELECT accurateCastOrNull(128, 'int8');

SELECT accurateCastOrNull(10, 'Decimal32(9)');
SELECT accurateCastOrNull(1, 'Decimal32(9)');
SELECT accurateCastOrNull(-10, 'Decimal32(9)');

SELECT accurateCastOrNull('123', 'FixedString(2)');

SELECT accurateCastOrNull(inf, 'int64');
SELECT accurateCastOrNull(inf, 'Int128');
SELECT accurateCastOrNull(inf, 'Int256');
SELECT accurateCastOrNull(nan, 'int64');
SELECT accurateCastOrNull(nan, 'Int128');
SELECT accurateCastOrNull(nan, 'Int256');

SELECT accurateCastOrNull(inf, 'uint64');
SELECT accurateCastOrNull(inf, 'UInt256');
SELECT accurateCastOrNull(nan, 'uint64');
SELECT accurateCastOrNull(nan, 'UInt256');

SELECT accurateCastOrNull(number + 127, 'int8') AS x FROM numbers (2) ORDER BY x;
