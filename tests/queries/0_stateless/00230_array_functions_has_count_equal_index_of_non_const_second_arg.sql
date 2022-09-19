select has([0 as x], x);
select has([0 as x], materialize(x));
select has(materialize([0 as x]), x);
select has(materialize([0 as x]), materialize(x));

select has([to_string(0) as x], x);
select has([to_string(0) as x], materialize(x));
select has(materialize([to_string(0) as x]), x);
select has(materialize([to_string(0) as x]), materialize(x));

select has([to_uint64(0)], number) from system.numbers limit 10;
select has([to_uint64(0)], to_uint64(number % 3)) from system.numbers limit 10;
select has(materialize([to_uint64(0)]), number) from system.numbers limit 10;
select has(materialize([to_uint64(0)]), to_uint64(number % 3)) from system.numbers limit 10;

select has([to_string(0)], to_string(number)) from system.numbers limit 10;
select has([to_string(0)], to_string(number % 3)) from system.numbers limit 10;
select has(materialize([to_string(0)]), to_string(number)) from system.numbers limit 10;
select has(materialize([to_string(0)]), to_string(number % 3)) from system.numbers limit 10;

select 3 = countEqual([0 as x, 1, x, x], x);
select 3 = countEqual([0 as x, 1, x, x], materialize(x));
select 3 = countEqual(materialize([0 as x, 1, x, x]), x);
select 3 = countEqual(materialize([0 as x, 1, x, x]), materialize(x));

select 3 = countEqual([0 as x, 1, x, x], x) from system.numbers limit 10;
select 3 = countEqual([0 as x, 1, x, x], materialize(x)) from system.numbers limit 10;
select 3 = countEqual(materialize([0 as x, 1, x, x]), x) from system.numbers limit 10;
select 3 = countEqual(materialize([0 as x, 1, x, x]), materialize(x)) from system.numbers limit 10;

select 4 = indexOf([0, 1, 2, 3 as x], x);
select 4 = indexOf([0, 1, 2, 3 as x], materialize(x));
select 4 = indexOf(materialize([0, 1, 2, 3 as x]), x);
select 4 = indexOf(materialize([0, 1, 2, 3 as x]), materialize(x));

select 4 = indexOf([0, 1, 2, 3 as x], x) from system.numbers limit 10;
select 4 = indexOf([0, 1, 2, 3 as x], materialize(x)) from system.numbers limit 10;
select 4 = indexOf(materialize([0, 1, 2, 3 as x]), x) from system.numbers limit 10;
select 4 = indexOf(materialize([0, 1, 2, 3 as x]), materialize(x)) from system.numbers limit 10;
