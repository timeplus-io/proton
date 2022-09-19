SELECT accurateCast(1e35, 'uint32'); -- { serverError 70 }
SELECT accurateCast(1e35, 'uint64'); -- { serverError 70 }
SELECT accurateCast(1e35, 'UInt128'); -- { serverError 70 }
SELECT accurateCast(1e35, 'UInt256'); -- { serverError 70 }

SELECT accurateCast(1e19, 'uint64');
SELECT accurateCast(1e19, 'UInt128');
SELECT accurateCast(1e19, 'UInt256');
SELECT accurateCast(1e20, 'uint64'); -- { serverError 70 }
SELECT accurateCast(1e20, 'UInt128'); -- { serverError 70 }
SELECT accurateCast(1e20, 'UInt256'); -- { serverError 70 }

SELECT accurateCast(1e19, 'int64'); -- { serverError 70 }
SELECT accurateCast(1e19, 'Int128');
SELECT accurateCast(1e19, 'Int256');
