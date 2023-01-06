SELECT to_decimal32('42.42', 4) AS x, to_decimal32(log(x), 4) AS y, round(exp(y), 6);
SELECT to_decimal32('42.42', 4) AS x, to_decimal32(log2(x), 4) AS y, round(exp2(y), 6);
SELECT to_decimal32('42.42', 4) AS x, to_decimal32(log10(x), 4) AS y, round(exp10(y), 6);

SELECT to_decimal32('42.42', 4) AS x, to_decimal32(sqrt(x), 3) AS y, y * y;
SELECT to_decimal32('42.42', 4) AS x, to_decimal32(cbrt(x), 4) AS y, to_decimal64(y, 4) * y * y;
SELECT to_decimal32('1.0', 5) AS x, erf(x), erfc(x);
SELECT to_decimal32('42.42', 4) AS x, round(lgamma(x), 6), round(tgamma(x) / 1e50, 6);

SELECT to_decimal32('0.0', 2) AS x, round(sin(x), 8), round(cos(x), 8), round(tan(x), 8);
SELECT to_decimal32(pi(), 8) AS x, round(sin(x), 8), round(cos(x), 8), round(tan(x), 8);
SELECT to_decimal32('1.0', 2) AS x, asin(x), acos(x), atan(x);


SELECT to_decimal64('42.42', 4) AS x, to_decimal32(log(x), 4) AS y, round(exp(y), 6);
SELECT to_decimal64('42.42', 4) AS x, to_decimal32(log2(x), 4) AS y, round(exp2(y), 6);
SELECT to_decimal64('42.42', 4) AS x, to_decimal32(log10(x), 4) AS y, round(exp10(y), 6);

SELECT to_decimal64('42.42', 4) AS x, to_decimal32(sqrt(x), 3) AS y, y * y;
SELECT to_decimal64('42.42', 4) AS x, to_decimal32(cbrt(x), 4) AS y, to_decimal64(y, 4) * y * y;
SELECT to_decimal64('1.0', 5) AS x, erf(x), erfc(x);
SELECT to_decimal64('42.42', 4) AS x, round(lgamma(x), 6), round(tgamma(x) / 1e50, 6);

SELECT to_decimal64('0.0', 2) AS x, round(sin(x), 8), round(cos(x), 8), round(tan(x), 8);
SELECT to_decimal64(pi(), 17) AS x, round(sin(x), 8), round(cos(x), 8), round(tan(x), 8);
SELECT to_decimal64('1.0', 2) AS x, asin(x), acos(x), atan(x);


SELECT to_decimal128('42.42', 4) AS x, to_decimal32(log(x), 4) AS y, round(exp(y), 6);
SELECT to_decimal128('42.42', 4) AS x, to_decimal32(log2(x), 4) AS y, round(exp2(y), 6);
SELECT to_decimal128('42.42', 4) AS x, to_decimal32(log10(x), 4) AS y, round(exp10(y), 6);

SELECT to_decimal128('42.42', 4) AS x, to_decimal32(sqrt(x), 3) AS y, y * y;
SELECT to_decimal128('42.42', 4) AS x, to_decimal32(cbrt(x), 4) AS y, to_decimal64(y, 4) * y * y;
SELECT to_decimal128('1.0', 5) AS x, erf(x), erfc(x);
SELECT to_decimal128('42.42', 4) AS x, round(lgamma(x), 6), round(tgamma(x) / 1e50, 6);

SELECT to_decimal128('0.0', 2) AS x, round(sin(x), 8), round(cos(x), 8), round(tan(x), 8);
SELECT to_decimal128(pi(), 14) AS x, round(sin(x), 8), round(cos(x), 8), round(tan(x), 8);
SELECT to_decimal128('1.0', 2) AS x, asin(x), acos(x), atan(x);


SELECT to_decimal32('4.2', 1) AS x, pow(x, 2), pow(x, 0.5); -- { serverError 43 }
SELECT to_decimal64('4.2', 1) AS x, pow(x, 2), pow(x, 0.5); -- { serverError 43 }
SELECT to_decimal128('4.2', 1) AS x, pow(x, 2), pow(x, 0.5); -- { serverError 43 }
