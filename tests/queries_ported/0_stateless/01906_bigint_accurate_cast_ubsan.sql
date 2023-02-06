SELECT accurate_cast(1e35, 'uint32'); -- { serverError 70 }
SELECT accurate_cast(1e35, 'uint64'); -- { serverError 70 }
SELECT accurate_cast(1e35, 'uint128'); -- { serverError 70 }
SELECT accurate_cast(1e35, 'uint256'); -- { serverError 70 }

SELECT accurate_cast(1e19, 'uint64');
SELECT accurate_cast(1e19, 'uint128');
SELECT accurate_cast(1e19, 'uint256');
SELECT accurate_cast(1e20, 'uint64'); -- { serverError 70 }
SELECT accurate_cast(1e20, 'uint128'); -- { serverError 70 }
SELECT accurate_cast(1e20, 'uint256'); -- { serverError 70 }

SELECT accurate_cast(1e19, 'int64'); -- { serverError 70 }
SELECT accurate_cast(1e19, 'int128');
SELECT accurate_cast(1e19, 'int256');
