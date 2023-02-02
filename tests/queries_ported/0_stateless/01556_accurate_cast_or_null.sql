SELECT accurate_cast_or_null(-1, 'uint8');
SELECT accurate_cast_or_null(5, 'uint8');
SELECT accurate_cast_or_null(257, 'uint8');
SELECT accurate_cast_or_null(-1, 'uint16');
SELECT accurate_cast_or_null(5, 'uint16');
SELECT accurate_cast_or_null(65536, 'uint16');
SELECT accurate_cast_or_null(-1, 'uint32');
SELECT accurate_cast_or_null(5, 'uint32');
SELECT accurate_cast_or_null(4294967296, 'uint32');
SELECT accurate_cast_or_null(-1, 'uint64');
SELECT accurate_cast_or_null(5, 'uint64');
SELECT accurate_cast_or_null(-1, 'uint256');
SELECT accurate_cast_or_null(5, 'uint256');

SELECT accurate_cast_or_null(-129, 'int8');
SELECT accurate_cast_or_null(5, 'int8');
SELECT accurate_cast_or_null(128, 'int8');

SELECT accurate_cast_or_null(10, 'decimal32(9)');
SELECT accurate_cast_or_null(1, 'decimal32(9)');
SELECT accurate_cast_or_null(-10, 'decimal32(9)');

SELECT accurate_cast_or_null('123', 'fixed_string(2)');

SELECT accurate_cast_or_null(inf, 'int64');
SELECT accurate_cast_or_null(inf, 'int128');
SELECT accurate_cast_or_null(inf, 'int256');
SELECT accurate_cast_or_null(nan, 'int64');
SELECT accurate_cast_or_null(nan, 'int128');
SELECT accurate_cast_or_null(nan, 'int256');

SELECT accurate_cast_or_null(inf, 'uint64');
SELECT accurate_cast_or_null(inf, 'uint256');
SELECT accurate_cast_or_null(nan, 'uint64');
SELECT accurate_cast_or_null(nan, 'uint256');

SELECT accurate_cast_or_null(number + 127, 'int8') AS x FROM numbers (2) ORDER BY x;
