SELECT to_decimal32OrZero('1.1', 1), to_decimal32OrZero('1.1', 2), to_decimal32OrZero('1.1', 8);
SELECT to_decimal32OrZero('1.1', 0);
SELECT to_decimal32OrZero(1.1, 0); -- { serverError 43 }

SELECT to_decimal128OrZero('', 0) AS x, to_decimal128OrZero('0.42', 2) AS y;
SELECT to_decimal64OrZero('', 0) AS x, to_decimal64OrZero('0.42', 3) AS y;
SELECT to_decimal32OrZero('', 0) AS x, to_decimal32OrZero('0.42', 4) AS y;

SELECT to_decimal32OrZero('999999999', 0), to_decimal32OrZero('1000000000', 0);
SELECT to_decimal32OrZero('-999999999', 0), to_decimal32OrZero('-1000000000', 0);
SELECT to_decimal64OrZero('999999999999999999', 0), to_decimal64OrZero('1000000000000000000', 0);
SELECT to_decimal64OrZero('-999999999999999999', 0), to_decimal64OrZero('-1000000000000000000', 0);
SELECT to_decimal128OrZero('99999999999999999999999999999999999999', 0);
SELECT to_decimal64OrZero('100000000000000000000000000000000000000', 0);
SELECT to_decimal128OrZero('-99999999999999999999999999999999999999', 0);
SELECT to_decimal64OrZero('-100000000000000000000000000000000000000', 0);

SELECT to_decimal32OrZero('1', rowNumberInBlock()); -- { serverError 44 }
SELECT to_decimal64OrZero('1', rowNumberInBlock()); -- { serverError 44 }
SELECT to_decimal128OrZero('1', rowNumberInBlock()); -- { serverError 44 }

SELECT '----';

SELECT to_decimal32OrNull('1.1', 1), to_decimal32OrNull('1.1', 2), to_decimal32OrNull('1.1', 8);
SELECT to_decimal32OrNull('1.1', 0);
SELECT to_decimal32OrNull(1.1, 0); -- { serverError 43 }

SELECT to_decimal128OrNull('', 0) AS x, to_decimal128OrNull('-0.42', 2) AS y;
SELECT to_decimal64OrNull('', 0) AS x, to_decimal64OrNull('-0.42', 3) AS y;
SELECT to_decimal32OrNull('', 0) AS x, to_decimal32OrNull('-0.42', 4) AS y;

SELECT to_decimal32OrNull('999999999', 0), to_decimal32OrNull('1000000000', 0);
SELECT to_decimal32OrNull('-999999999', 0), to_decimal32OrNull('-1000000000', 0);
SELECT to_decimal64OrNull('999999999999999999', 0), to_decimal64OrNull('1000000000000000000', 0);
SELECT to_decimal64OrNull('-999999999999999999', 0), to_decimal64OrNull('-1000000000000000000', 0);
SELECT to_decimal128OrNull('99999999999999999999999999999999999999', 0);
SELECT to_decimal64OrNull('100000000000000000000000000000000000000', 0);
SELECT to_decimal128OrNull('-99999999999999999999999999999999999999', 0);
SELECT to_decimal64OrNull('-100000000000000000000000000000000000000', 0);

SELECT to_decimal32OrNull('1', rowNumberInBlock()); -- { serverError 44 }
SELECT to_decimal64OrNull('1', rowNumberInBlock()); -- { serverError 44 }
SELECT to_decimal128OrNull('1', rowNumberInBlock()); -- { serverError 44 }
