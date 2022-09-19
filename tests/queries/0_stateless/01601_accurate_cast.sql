SELECT accurateCast(-1, 'uint8'); -- { serverError 70 }
SELECT accurateCast(5, 'uint8');
SELECT accurateCast(257, 'uint8'); -- { serverError 70 }
SELECT accurateCast(-1, 'uint16'); -- { serverError 70 }
SELECT accurateCast(5, 'uint16');
SELECT accurateCast(65536, 'uint16'); -- { serverError 70 }
SELECT accurateCast(-1, 'uint32'); -- { serverError 70 }
SELECT accurateCast(5, 'uint32');
SELECT accurateCast(4294967296, 'uint32'); -- { serverError 70 }
SELECT accurateCast(-1, 'uint64'); -- { serverError 70 }
SELECT accurateCast(5, 'uint64');
SELECT accurateCast(-1, 'UInt256'); -- { serverError 70 }
SELECT accurateCast(5, 'UInt256');

SELECT accurateCast(-129, 'int8'); -- { serverError 70 }
SELECT accurateCast(5, 'int8');
SELECT accurateCast(128, 'int8'); -- { serverError 70 }

SELECT accurateCast(10, 'Decimal32(9)'); -- { serverError 407 }
SELECT accurateCast(1, 'Decimal32(9)');
SELECT accurateCast(-10, 'Decimal32(9)'); -- { serverError 407 }

SELECT accurateCast('123', 'FixedString(2)'); -- { serverError 131 }
SELECT accurateCast('12', 'FixedString(2)');
