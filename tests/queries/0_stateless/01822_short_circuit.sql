set short_circuit_function_evaluation = 'enable';

select if(number > 0, int_div(number + 100, number), throwIf(number)) from numbers(10);
select multi_if(number == 0, 0, number == 1, int_div(1, number), number == 2, int_div(1, number - 1), number == 3, int_div(1, number - 2), int_div(1, number - 3)) from numbers(10);
select number != 0 and int_div(1, number) == 0 and number != 2 and int_div(1, number - 2) == 0 from numbers(10);
select number == 0 or int_div(1, number) != 0 or number == 2 or int_div(1, number - 2) != 0 from numbers(10);

select count() from (select if(number >= 0, number, sleep(1)) from numbers(10000000));


select if(number % 5 == 0, to_int8OrZero(to_string(number)), to_int8OrZero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, to_int8OrZero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, to_int8OrZero(to_string(number))) from numbers(20);
select if(number % 5, Null, to_int8OrZero(to_string(number))) from numbers(20);

select if(number % 5 == 0, toUInt8OrZero(to_string(number)), toUInt8OrZero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt8OrZero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toUInt8OrZero(to_string(number))) from numbers(20);
select if(number % 5, Null, toUInt8OrZero(to_string(number))) from numbers(20);

select if(number % 5 == 0, to_int32OrZero(to_string(number)), to_int32OrZero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, to_int32OrZero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, to_int32OrZero(to_string(number))) from numbers(20);
select if(number % 5, Null, to_int32OrZero(to_string(number))) from numbers(20);

select if(number % 5 == 0, to_int32_or_zero(to_string(number)), to_int32_or_zero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, to_int32_or_zero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, to_int32_or_zero(to_string(number))) from numbers(20);
select if(number % 5, Null, to_int32_or_zero(to_string(number))) from numbers(20);

select if(number % 5 == 0, to_int64OrZero(to_string(number)), to_int64OrZero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, to_int64OrZero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, to_int64OrZero(to_string(number))) from numbers(20);
select if(number % 5, Null, to_int64OrZero(to_string(number))) from numbers(20);

select if(number % 5 == 0, to_int64_or_zero(to_string(number)), to_int64_or_zero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, to_int64_or_zero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, to_int64_or_zero(to_string(number))) from numbers(20);
select if(number % 5, Null, to_int64_or_zero(to_string(number))) from numbers(20);

select if(number % 5 == 0, to_int128OrZero(to_string(number)), to_int128OrZero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, to_int128OrZero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, to_int128OrZero(to_string(number))) from numbers(20);
select if(number % 5, Null, to_int128OrZero(to_string(number))) from numbers(20);

select if(number % 5 == 0, toUInt128OrZero(to_string(number)), toUInt128OrZero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt128OrZero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toUInt128OrZero(to_string(number))) from numbers(20);
select if(number % 5, Null, toUInt128OrZero(to_string(number))) from numbers(20);

select if(number % 5 == 0, toInt256OrZero(to_string(number)), toInt256OrZero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, toInt256OrZero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toInt256OrZero(to_string(number))) from numbers(20);
select if(number % 5, Null, toInt256OrZero(to_string(number))) from numbers(20);

select if(number % 5 == 0, toUInt256OrZero(to_string(number)), toUInt256OrZero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt256OrZero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toUInt256OrZero(to_string(number))) from numbers(20);
select if(number % 5, Null, toUInt256OrZero(to_string(number))) from numbers(20);

select if(number % 5 == 0, to_float32_or_zero(to_string(number)), to_float32_or_zero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, to_float32_or_zero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, to_float32_or_zero(to_string(number))) from numbers(20);
select if(number % 5, Null, to_float32_or_zero(to_string(number))) from numbers(20);

select if(number % 5 == 0, to_float64_or_zero(to_string(number)), to_float64_or_zero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, to_float64_or_zero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, to_float64_or_zero(to_string(number))) from numbers(20);
select if(number % 5, Null, to_float64_or_zero(to_string(number))) from numbers(20);

select if(number % 5 == 0, repeat(to_string(number), 2), repeat(to_string(number + 1), 2)) from numbers(20);
select if(number % 5 == 0, repeat(to_string(number), 2), Null) from numbers(20);
select if(number % 5 == 0, Null, repeat(to_string(number), 2)) from numbers(20);
select if(number % 5, Null, repeat(to_string(number), 2)) from numbers(20);

select if(number % 5 == 0, to_fixed_string(to_string(number + 10), 2), to_fixed_string(to_string(number + 11), 2)) from numbers(20);
select if(number % 5 == 0, to_fixed_string(to_string(number + 10), 2), Null) from numbers(20);
select if(number % 5 == 0, Null, to_fixed_string(to_string(number + 10), 2)) from numbers(20);
select if(number % 5, Null, to_fixed_string(to_string(number + 10), 2)) from numbers(20);

select if(number % 5 == 0, toDateOrZero(to_string(number)), toDateOrZero(to_string(number + 1))) from numbers(20);
select if(number % 5 == 0, toDateOrZero(to_string(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toDateOrZero(to_string(number))) from numbers(20);
select if(number % 5, Null, toDateOrZero(to_string(number))) from numbers(20);

select if(number % 5 == 0, toDateTimeOrZero(to_string(number * 10000), 'UTC'), toDateTimeOrZero(to_string((number + 1) * 10000), 'UTC')) from numbers(20);
select if(number % 5 == 0, toDateTimeOrZero(to_string(number * 10000), 'UTC'), Null) from numbers(20);
select if(number % 5 == 0, Null, toDateTimeOrZero(to_string(number * 10000), 'UTC')) from numbers(20);
select if(number % 5, Null, toDateTimeOrZero(to_string(number * 10000), 'UTC')) from numbers(20);

select if(number % 5 == 0, toDecimal32OrZero(to_string(number), 5), toDecimal32OrZero(to_string(number + 1), 5)) from numbers(20);
select if(number % 5 == 0, toDecimal32OrZero(to_string(number), 5), Null) from numbers(20);
select if(number % 5 == 0, Null, toDecimal32OrZero(to_string(number), 5)) from numbers(20);
select if(number % 5, Null, toDecimal32OrZero(to_string(number), 5)) from numbers(20);

select if(number % 5 == 0, toDecimal64OrZero(to_string(number), 5), toDecimal64OrZero(to_string(number + 1), 5)) from numbers(20);
select if(number % 5 == 0, toDecimal64OrZero(to_string(number), 5), Null) from numbers(20);
select if(number % 5 == 0, Null, toDecimal64OrZero(to_string(number), 5)) from numbers(20);
select if(number % 5, Null, toDecimal64OrZero(to_string(number), 5)) from numbers(20);

select if(number % 5 == 0, toDecimal128OrZero(to_string(number), 5), toDecimal128OrZero(to_string(number + 1), 5)) from numbers(20);
select if(number % 5 == 0, toDecimal128OrZero(to_string(number), 5), Null) from numbers(20);
select if(number % 5 == 0, Null, toDecimal128OrZero(to_string(number), 5)) from numbers(20);
select if(number % 5, Null, toDecimal128OrZero(to_string(number), 5)) from numbers(20);

select if(number % 5 == 0, toDecimal256OrZero(to_string(number), 5), toDecimal256OrZero(to_string(number + 1), 5)) from numbers(20);
select if(number % 5 == 0, toDecimal256OrZero(to_string(number), 5), Null) from numbers(20);
select if(number % 5 == 0, Null, toDecimal256OrZero(to_string(number), 5)) from numbers(20);
select if(number % 5, Null, toDecimal256OrZero(to_string(number), 5)) from numbers(20);

select if(number % 5 == 0, range(number), range(number + 1)) from numbers(20);
select if(number % 5 == 0, replicate(to_string(number), range(number)), replicate(to_string(number), range(number + 1))) from numbers(20);

select number > 0 and 5 and int_div(100, number) from numbers(5);
select number > 0 and Null and int_div(100, number) from numbers(5);
select number == 0 or 5 or int_div(100, number) from numbers(5);
select multi_if(number % 2 != 0, int_div(10, number % 2), 5, int_div(10, 1 - number % 2), int_div(10, number)) from numbers(5);

select if(number != 0, 5 * (1 + int_div(100, number)), to_int32(exp(log(throwIf(number) + 10)))) from numbers(5);
select if(number % 2, 5 * (1 + int_div(100, number + 1)), 3 + 10 * int_div(100, int_div(100, number + 1))) from numbers(10);

select sum(number) FROM numbers(10) WHERE number != 0 and 3 % number and number != 1 and int_div(1, number - 1) > 0;
select multi_if(0, 1, int_div(number % 2, 1), 2, 0, 3, 1, number + 10, 2) from numbers(10);

select to_type_name(to_string(number)) from numbers(5);
select to_type_name(to_string(number)) from numbers(5);

select to_type_name(to_int64OrZero(to_string(number))) from numbers(5);
select to_type_name(to_int64OrZero(to_string(number))) from numbers(5);

select to_type_name(toDecimal32OrZero(to_string(number), 5)) from numbers(5);
select to_type_name(toDecimal32OrZero(to_string(number), 5)) from numbers(5);

select if(if(number > 0, int_div(42, number), 0), int_div(42, number), 8) from numbers(5);
select if(number > 0, int_div(42, number), 0), if(number = 0, 0, int_div(42, number)) from numbers(5);

select Null or is_null(int_div(number, 1)) from numbers(5);

set compile_expressions = 1;
select if(number > 0, int_div(42, number), 1) from numbers(5);
select if(number > 0, int_div(42, number), 1) from numbers(5);
select if(number > 0, int_div(42, number), 1) from numbers(5);
select if(number > 0, int_div(42, number), 1) from numbers(5);

select if(number > 0, 42 / to_decimal32(number, 2), 0) from numbers(5);
select if(number = 0, 0, to_decimal32(42, 2) / number) from numbers(5);
select if(is_null(x), Null, 42 / x) from (select CAST(materialize(Null), 'nullable(Decimal32(2))') as x);
select if(is_null(x), Null, x / 0) from (select CAST(materialize(Null), 'nullable(Decimal32(2))') as x);

select if(is_null(x), Null, int_div(42, x)) from (select CAST(materialize(Null), 'nullable(int64)') as x);

select number % 2 and toLowCardinality(number) from numbers(5);
select number % 2 or toLowCardinality(number) from numbers(5);
select if(toLowCardinality(number) % 2, number, number + 1) from numbers(10);
select multi_if(toLowCardinality(number) % 2, number, number + 1) from numbers(10);

