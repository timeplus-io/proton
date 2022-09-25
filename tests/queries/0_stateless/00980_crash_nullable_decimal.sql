select array_reduce('median', [toDecimal32OrNull('1', 2)]);
select array_reduce('median', [toDecimal64OrNull('1', 2)]);
select array_reduce('median', [toDecimal128OrZero('1', 2)]);
select array_reduce('sum', [toDecimal128OrNull('1', 2)]);

select array_reduce('median', [toDecimal128OrNull('1', 2)]);
select array_reduce('quantile(0.2)', [toDecimal128OrNull('1', 2)]);
select array_reduce('median_exact', [toDecimal128OrNull('1', 2)]);
