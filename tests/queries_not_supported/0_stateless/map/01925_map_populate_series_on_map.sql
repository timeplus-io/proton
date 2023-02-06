-- { echo }
drop stream if exists map_test;
set allow_experimental_map_type = 1;
create stream map_test engine=TinyLog() as (select (number + 1) as n, map(1, 1, number,2) as m from numbers(1, 5));

select mapPopulateSeries(m) from map_test;
select mapPopulateSeries(m, to_uint64(3)) from map_test;
select mapPopulateSeries(m, to_uint64(10)) from map_test;
select mapPopulateSeries(m, 10) from map_test;
select mapPopulateSeries(m, n) from map_test;

drop stream map_test;

select mapPopulateSeries(map(to_uint8(1), to_uint8(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_uint16(1), to_uint16(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_uint32(1), to_uint32(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_uint64(1), to_uint64(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_uint128(1), to_uint128(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_uint256(1), to_uint256(1), 2, 1)) as res, to_type_name(res);

select mapPopulateSeries(map(to_int8(1), to_int8(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_int16(1), to_int16(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_int32(1), to_int32(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_int64(1), to_int64(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_int128(1), to_int128(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_int256(1), to_int256(1), 2, 1)) as res, to_type_name(res);

select mapPopulateSeries(map(to_int8(-10), to_int8(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_int16(-10), to_int16(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_int32(-10), to_int32(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_int64(-10), to_int64(1), 2, 1)) as res, to_type_name(res);
select mapPopulateSeries(map(to_int64(-10), to_int64(1), 2, 1), to_int64(-5)) as res, to_type_name(res);

select mapPopulateSeries(); -- { serverError 42 }
select mapPopulateSeries('asdf'); -- { serverError 43 }
select mapPopulateSeries(map('1', 1, '2', 1)) as res, to_type_name(res); -- { serverError 43 }
