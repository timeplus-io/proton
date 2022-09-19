SET query_mode = 'table';
drop stream if exists map_test;
create stream map_test engine=TinyLog() as (select ([1, number], [to_int32(2),2]) as map from numbers(1, 10));

-- mapAdd
select mapAdd([1], [1]); -- { serverError 43 }
select mapAdd(([1], [1])); -- { serverError 42 }
select mapAdd(([1], [1]), map) from map_test; -- { serverError 43 }
select mapAdd(([to_uint64(1)], [1]), map) from map_test; -- { serverError 43 }
select mapAdd(([to_uint64(1), 2], [to_int32(1)]), map) from map_test; -- {serverError 42 }

select mapAdd(([to_uint64(1)], [to_int32(1)]), map) from map_test;
select mapAdd(cast(map, 'tuple(array(uint8), array(uint8))'), ([1], [1]), ([2],[2]) ) from map_test;

-- cleanup
drop stream map_test;

-- check types
select mapAdd(([to_uint8(1), 2], [1, 1]), ([to_uint8(1), 2], [1, 1])) as res, to_type_name(res);
select mapAdd(([to_uint16(1), 2], [to_uint16(1), 1]), ([to_uint16(1), 2], [to_uint16(1), 1])) as res, to_type_name(res);
select mapAdd(([to_uint32(1), 2], [to_uint32(1), 1]), ([to_uint32(1), 2], [to_uint32(1), 1])) as res, to_type_name(res);
select mapAdd(([to_uint64(1), 2], [to_uint64(1), 1]), ([to_uint64(1), 2], [to_uint64(1), 1])) as res, to_type_name(res);

select mapAdd(([to_int8(1), 2], [to_int8(1), 1]), ([to_int8(1), 2], [to_int8(1), 1])) as res, to_type_name(res);
select mapAdd(([to_int16(1), 2], [to_int16(1), 1]), ([to_int16(1), 2], [to_int16(1), 1])) as res, to_type_name(res);
select mapAdd(([to_int32(1), 2], [to_int32(1), 1]), ([to_int32(1), 2], [to_int32(1), 1])) as res, to_type_name(res);
select mapAdd(([to_int64(1), 2], [to_int64(1), 1]), ([to_int64(1), 2], [to_int64(1), 1])) as res, to_type_name(res);

select mapAdd(([1, 2], [to_float32(1.1), 1]), ([1, 2], [2.2, 1])) as res, to_type_name(res);
select mapAdd(([1, 2], [toFloat64(1.1), 1]), ([1, 2], [2.2, 1])) as res, to_type_name(res);
select mapAdd(([to_float32(1), 2], [toFloat64(1.1), 1]), ([to_float32(1), 2], [2.2, 1])) as res, to_type_name(res); -- { serverError 43 }
select mapAdd(([1, 2], [toFloat64(1.1), 1]), ([1, 2], [1, 1])) as res, to_type_name(res); -- { serverError 43 }
select mapAdd((['a', 'b'], [1, 1]), ([key], [1])) from values('key string', ('b'), ('c'), ('d'));
select mapAdd((cast(['a', 'b'], 'array(FixedString(1))'), [1, 1]), ([key], [1])) as res, to_type_name(res) from values('key FixedString(1)', ('b'), ('c'), ('d'));
select mapAdd((cast(['a', 'b'], 'array(LowCardinality(string))'), [1, 1]), ([key], [1])) from values('key string', ('b'), ('c'), ('d'));
select mapAdd((key, val), (key, val)) as res, to_type_name(res) from values ('key array(Enum16(\'a\'=1, \'b\'=2)), val array(Int16)',  (['a'], [1]), (['b'], [1]));
select mapAdd((key, val), (key, val)) as res, to_type_name(res) from values ('key array(Enum8(\'a\'=1, \'b\'=2)), val array(Int16)',  (['a'], [1]), (['b'], [1]));
select mapAdd((key, val), (key, val)) as res, to_type_name(res) from values ('key array(UUID), val array(int32)', (['00000000-89ab-cdef-0123-456789abcdef'], [1]), (['11111111-89ab-cdef-0123-456789abcdef'], [2]));

-- mapSubtract, same rules as mapAdd
select mapSubtract(([to_uint8(1), 2], [1, 1]), ([to_uint8(1), 2], [1, 1])) as res, to_type_name(res);
select mapSubtract(([to_uint8(1), 2], [1, 1]), ([to_uint8(1), 2], [2, 2])) as res, to_type_name(res); -- overflow
select mapSubtract(([to_uint8(1), 2], [to_int32(1), 1]), ([to_uint8(1), 2], [to_int16(2), 2])) as res, to_type_name(res);
select mapSubtract(([1, 2], [to_float32(1.1), 1]), ([1, 2], [2.2, 1])) as res, to_type_name(res);
select mapSubtract(([to_uint8(1), 2], [to_int32(1), 1]), ([to_uint8(1), 2], [to_int16(2), 2])) as res, to_type_name(res);
select mapSubtract(([to_uint8(3)], [to_int32(1)]), ([to_uint8(1), 2], [to_int32(2), 2])) as res, to_type_name(res);
