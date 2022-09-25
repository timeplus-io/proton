SET query_mode = 'table';
drop stream if exists map_test;
create stream map_test engine=TinyLog() as (select (number + 1) as n, ([1, number], [1,2]) as map from numbers(1, 5));

select mapPopulateSeries(map.1, map.2) from map_test;
select mapPopulateSeries(map.1, map.2, to_uint64(3)) from map_test;
select mapPopulateSeries(map.1, map.2, to_uint64(10)) from map_test;
select mapPopulateSeries(map.1, map.2, 1000) from map_test; -- { serverError 43 }
select mapPopulateSeries(map.1, map.2, n) from map_test;
select mapPopulateSeries(map.1, [11,22]) from map_test;
select mapPopulateSeries([3, 4], map.2) from map_test;
select mapPopulateSeries([to_uint64(3), 4], map.2, n) from map_test;

drop stream map_test;

select mapPopulateSeries([to_uint8(1), 2], [to_uint8(1), 1]) as res, to_type_name(res);
select mapPopulateSeries([to_uint16(1), 2], [to_uint16(1), 1]) as res, to_type_name(res);
select mapPopulateSeries([to_uint32(1), 2], [to_uint32(1), 1]) as res, to_type_name(res);
select mapPopulateSeries([to_uint64(1), 2], [to_uint64(1), 1]) as res, to_type_name(res);

select mapPopulateSeries([to_int8(1), 2], [to_int8(1), 1]) as res, to_type_name(res);
select mapPopulateSeries([to_int16(1), 2], [to_int16(1), 1]) as res, to_type_name(res);
select mapPopulateSeries([to_int32(1), 2], [to_int32(1), 1]) as res, to_type_name(res);
select mapPopulateSeries([to_int64(1), 2], [to_int64(1), 1]) as res, to_type_name(res);

select mapPopulateSeries([to_int8(-10), 2], [to_int8(1), 1]) as res, to_type_name(res);
select mapPopulateSeries([to_int16(-10), 2], [to_int16(1), 1]) as res, to_type_name(res);
select mapPopulateSeries([to_int32(-10), 2], [to_int32(1), 1]) as res, to_type_name(res);
select mapPopulateSeries([to_int64(-10), 2], [to_int64(1), 1]) as res, to_type_name(res);
select mapPopulateSeries([to_int64(-10), 2], [to_int64(1), 1], to_int64(-5)) as res, to_type_name(res);

-- empty
select mapPopulateSeries(cast([], 'array(uint8)'), cast([], 'array(uint8)'), 5);

select mapPopulateSeries(['1', '2'], [1,1]) as res, to_type_name(res); -- { serverError 43 }
select mapPopulateSeries([1, 2, 3], [1,1]) as res, to_type_name(res); -- { serverError 42 }
select mapPopulateSeries([1, 2], [1,1,1]) as res, to_type_name(res); -- { serverError 42 }
