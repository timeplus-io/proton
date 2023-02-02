SELECT accurate_cast(-1, 'uint8'); -- { serverError 70 }
SELECT accurate_cast(5, 'uint8');
SELECT accurate_cast(257, 'uint8'); -- { serverError 70 }
SELECT accurate_cast(-1, 'uint16'); -- { serverError 70 }
SELECT accurate_cast(5, 'uint16');
SELECT accurate_cast(65536, 'uint16'); -- { serverError 70 }
SELECT accurate_cast(-1, 'uint32'); -- { serverError 70 }
SELECT accurate_cast(5, 'uint32');
SELECT accurate_cast(4294967296, 'uint32'); -- { serverError 70 }
SELECT accurate_cast(-1, 'uint64'); -- { serverError 70 }
SELECT accurate_cast(5, 'uint64');
SELECT accurate_cast(-1, 'uint256'); -- { serverError 70 }
SELECT accurate_cast(5, 'uint256');

SELECT accurate_cast(-129, 'int8'); -- { serverError 70 }
SELECT accurate_cast(5, 'int8');
SELECT accurate_cast(128, 'int8'); -- { serverError 70 }

SELECT accurate_cast(10, 'decimal32(9)'); -- { serverError 407 }
SELECT accurate_cast(1, 'decimal32(9)');
SELECT accurate_cast(-10, 'decimal32(9)'); -- { serverError 407 }

SELECT accurate_cast('123', 'fixed_string(2)'); -- { serverError 131 }
SELECT accurate_cast('12', 'fixed_string(2)');
