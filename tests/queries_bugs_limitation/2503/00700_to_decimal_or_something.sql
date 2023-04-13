SELECT to_decimal32_or_zero('1.1', 1), to_decimal32_or_zero('1.1', 2), to_decimal32_or_zero('1.1', 8);
SELECT to_decimal32_or_zero('1.1', 0);
SELECT to_decimal32_or_zero(1.1, 0); -- { serverError 43 }

SELECT to_decimal128_or_zero('', 0) AS x, to_decimal128_or_zero('0.42', 2) AS y;
SELECT to_decimal64_or_zero('', 0) AS x, to_decimal64_or_zero('0.42', 3) AS y;
SELECT to_decimal32_or_zero('', 0) AS x, to_decimal32_or_zero('0.42', 4) AS y;

SELECT to_decimal32_or_zero('999999999', 0), to_decimal32_or_zero('1000000000', 0);
SELECT to_decimal32_or_zero('-999999999', 0), to_decimal32_or_zero('-1000000000', 0);
SELECT to_decimal64_or_zero('999999999999999999', 0), to_decimal64_or_zero('1000000000000000000', 0);
SELECT to_decimal64_or_zero('-999999999999999999', 0), to_decimal64_or_zero('-1000000000000000000', 0);
SELECT to_decimal128_or_zero('99999999999999999999999999999999999999', 0);
SELECT to_decimal64_or_zero('100000000000000000000000000000000000000', 0);
SELECT to_decimal128_or_zero('-99999999999999999999999999999999999999', 0);
SELECT to_decimal64_or_zero('-100000000000000000000000000000000000000', 0);

SELECT to_decimal32_or_zero('1', row_number_in_block()); -- { serverError 44 }
SELECT to_decimal64_or_zero('1', row_number_in_block()); -- { serverError 44 }
SELECT to_decimal128_or_zero('1', row_number_in_block()); -- { serverError 44 }

SELECT '----';

SELECT to_decimal32_or_null('1.1', 1), to_decimal32_or_null('1.1', 2), to_decimal32_or_null('1.1', 8);
SELECT to_decimal32_or_null('1.1', 0);
SELECT to_decimal32_or_null(1.1, 0); -- { serverError 43 }

SELECT to_decimal128_or_null('', 0) AS x, to_decimal128_or_null('-0.42', 2) AS y;
SELECT to_decimal64_or_null('', 0) AS x, to_decimal64_or_null('-0.42', 3) AS y;
SELECT to_decimal32_or_null('', 0) AS x, to_decimal32_or_null('-0.42', 4) AS y;

SELECT to_decimal32_or_null('999999999', 0), to_decimal32_or_null('1000000000', 0);
SELECT to_decimal32_or_null('-999999999', 0), to_decimal32_or_null('-1000000000', 0);
SELECT to_decimal64_or_null('999999999999999999', 0), to_decimal64_or_null('1000000000000000000', 0);
SELECT to_decimal64_or_null('-999999999999999999', 0), to_decimal64_or_null('-1000000000000000000', 0);
SELECT to_decimal128_or_null('99999999999999999999999999999999999999', 0);
SELECT to_decimal64_or_null('100000000000000000000000000000000000000', 0);
SELECT to_decimal128_or_null('-99999999999999999999999999999999999999', 0);
SELECT to_decimal64_or_null('-100000000000000000000000000000000000000', 0);

SELECT to_decimal32_or_null('1', row_number_in_block()); -- { serverError 44 }
SELECT to_decimal64_or_null('1', row_number_in_block()); -- { serverError 44 }
SELECT to_decimal128_or_null('1', row_number_in_block()); -- { serverError 44 }
