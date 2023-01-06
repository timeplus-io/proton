select array_reduce('median', [to_decimal32OrNull('1', 2)]);
select array_reduce('median', [to_decimal64OrNull('1', 2)]);
select array_reduce('median', [to_decimal128OrZero('1', 2)]);
select array_reduce('sum', [to_decimal128OrNull('1', 2)]);

select array_reduce('median', [to_decimal128OrNull('1', 2)]);
select array_reduce('quantile(0.2)', [to_decimal128OrNull('1', 2)]);
select array_reduce('median_exact', [to_decimal128OrNull('1', 2)]);
