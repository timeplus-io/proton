-- Tags: no-fasttest

-- check cases when one of operands is zero
SELECT divide_decimal(to_decimal32(0, 2), to_decimal128(11.123456, 6));
SELECT divide_decimal(to_decimal64(123.123, 3), to_decimal64(0, 1)); -- { serverError 153 }
SELECT multiply_decimal(to_decimal32(0, 2), to_decimal128(11.123456, 6));
SELECT multiply_decimal(to_decimal32(123.123, 3), to_decimal128(0, 1));

-- don't look at strange query result -- it happens due to bad float precision: to_uint256(1e38) == 99999999999999997752612184630461283328
SELECT multiply_decimal(to_decimal256(1e38, 0), to_decimal256(1e38, 0));
SELECT divide_decimal(to_decimal256(1e66, 0), to_decimal256(1e-10, 10), 0);

-- fits decimal256, but scale is too big to fit
SELECT multiply_decimal(to_decimal256(1e38, 0), to_decimal256(1e38, 0), 2); -- { serverError 407 }
SELECT divide_decimal(to_decimal256(1e72, 0), to_decimal256(1e-5, 5), 2); -- { serverError 407 }

-- does not fit decimal256
SELECT multiply_decimal(to_decimal256('1e38', 0), to_decimal256('1e38', 0)); -- { serverError 407 }
SELECT multiply_decimal(to_decimal256(1e39, 0), to_decimal256(1e39, 0), 0); -- { serverError 407 }
SELECT divide_decimal(to_decimal256(1e39, 0), to_decimal256(1e-38, 39)); -- { serverError 407 }

-- test different signs
SELECT divide_decimal(to_decimal128(123.76, 2), to_decimal128(11.123456, 6));
SELECT divide_decimal(to_decimal32(123.123, 3), to_decimal128(11.4, 1), 2);
SELECT divide_decimal(to_decimal128(-123.76, 2), to_decimal128(11.123456, 6));
SELECT divide_decimal(to_decimal32(123.123, 3), to_decimal128(-11.4, 1), 2);
SELECT divide_decimal(to_decimal32(-123.123, 3), to_decimal128(-11.4, 1), 2);

SELECT multiply_decimal(to_decimal64(123.76, 2), to_decimal128(11.123456, 6));
SELECT multiply_decimal(to_decimal32(123.123, 3), to_decimal128(11.4, 1), 2);
SELECT multiply_decimal(to_decimal64(-123.76, 2), to_decimal128(11.123456, 6));
SELECT multiply_decimal(to_decimal32(123.123, 3), to_decimal128(-11.4, 1), 2);
SELECT multiply_decimal(to_decimal32(-123.123, 3), to_decimal128(-11.4, 1), 2);

-- check against non-const columns
SELECT sum(multiply_decimal(to_decimal64(number, 1), to_decimal64(number, 5))) FROM numbers(1000);
SELECT sum(divide_decimal(to_decimal64(number, 1), to_decimal64(number, 5))) FROM (select * from numbers(1000) OFFSET 1);

-- check against nullable type
SELECT multiply_decimal(to_nullable(to_decimal64(10, 1)), to_decimal64(100, 5));
SELECT multiply_decimal(to_decimal64(10, 1), to_nullable(to_decimal64(100, 5)));
SELECT multiply_decimal(to_nullable(to_decimal64(10, 1)), to_nullable(to_decimal64(100, 5)));
SELECT divide_decimal(to_nullable(to_decimal64(10, 1)), to_decimal64(100, 5));
SELECT divide_decimal(to_decimal64(10, 1), to_nullable(to_decimal64(100, 5)));
SELECT divide_decimal(to_nullable(to_decimal64(10, 1)), to_nullable(to_decimal64(100, 5)));
