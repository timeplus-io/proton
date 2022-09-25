SET query_mode = 'table';
drop stream if exists mapop_test;
set allow_experimental_map_type = 1;
create stream mapop_test engine=TinyLog() as (select map(1, to_int32(2), number, 2) as m from numbers(1, 10));

-- mapAdd
select mapAdd(map(1, 1)); -- { serverError 42 }
select mapAdd(map(1, 1), m) from mapop_test; -- { serverError 43 }

select mapAdd(map(to_uint64(1), to_int32(1)), m) from mapop_test;
select mapAdd(cast(m, 'Map(uint8, uint8)'), map(1, 1), map(2,2)) from mapop_test;

-- cleanup
drop stream mapop_test;

-- check types
select mapAdd(map(to_uint8(1), 1, 2, 1), map(to_uint8(1), 1, 2, 1)) as res, to_type_name(res);
select mapAdd(map(to_uint16(1), to_uint16(1), 2, 1), map(to_uint16(1), to_uint16(1), 2, 1)) as res, to_type_name(res);
select mapAdd(map(to_uint32(1), to_uint32(1), 2, 1), map(to_uint32(1), to_uint32(1), 2, 1)) as res, to_type_name(res);
select mapAdd(map(to_uint64(1), to_uint64(1), 2, 1), map(to_uint64(1), to_uint64(1), 2, 1)) as res, to_type_name(res);
select mapAdd(map(toUInt128(1), toUInt128(1), 2, 1), map(toUInt128(1), toUInt128(1), 2, 1)) as res, to_type_name(res);
select mapAdd(map(toUInt256(1), toUInt256(1), 2, 1), map(toUInt256(1), toUInt256(1), 2, 1)) as res, to_type_name(res);

select mapAdd(map(to_int8(1), 1, 2, 1), map(to_int8(1), 1, 2, 1)) as res, to_type_name(res);
select mapAdd(map(to_int16(1), to_int16(1), 2, 1), map(to_int16(1), to_int16(1), 2, 1)) as res, to_type_name(res);
select mapAdd(map(to_int32(1), to_int32(1), 2, 1), map(to_int32(1), to_int32(1), 2, 1)) as res, to_type_name(res);
select mapAdd(map(to_int64(1), to_int64(1), 2, 1), map(to_int64(1), to_int64(1), 2, 1)) as res, to_type_name(res);
select mapAdd(map(to_int128(1), to_int128(1), 2, 1), map(to_int128(1), to_int128(1), 2, 1)) as res, to_type_name(res);
select mapAdd(map(toInt256(1), toInt256(1), 2, 1), map(toInt256(1), toInt256(1), 2, 1)) as res, to_type_name(res);

select mapAdd(map(1, to_float32(1.1), 2, 1), map(1, 2.2, 2, 1)) as res, to_type_name(res);
select mapAdd(map(1, to_float64(1.1), 2, 1), map(1, 2.2, 2, 1)) as res, to_type_name(res);
select mapAdd(map(1, to_float64(1.1), 2, 1), map(1, 1, 2, 1)) as res, to_type_name(res); -- { serverError 43 }
select mapAdd(map('a', 1, 'b', 1), map(key, 1)) from values('key string', ('b'), ('c'), ('d'));
select mapAdd(map(cast('a', 'fixed_string(1)'), 1, 'b', 1), map(key, 1)) as res, to_type_name(res) from values('key string', ('b'), ('c'), ('d'));
select mapAdd(map(cast('a', 'low_cardinality(string)'), 1, 'b', 1), map(key, 1)) from values('key string', ('b'), ('c'), ('d'));
select mapAdd(map(key, val), map(key, val)) as res, to_type_name(res) from values ('key Enum16(\'a\'=1, \'b\'=2), val int16',  ('a', 1), ('b', 1));
select mapAdd(map(key, val), map(key, val)) as res, to_type_name(res) from values ('key enum8(\'a\'=1, \'b\'=2), val int16',  ('a', 1), ('b', 1));
select mapAdd(map(key, val), map(key, val)) as res, to_type_name(res) from values ('key uuid, val int32', ('00000000-89ab-cdef-0123-456789abcdef', 1), ('11111111-89ab-cdef-0123-456789abcdef', 2));

-- mapSubtract, same rules as mapAdd
select mapSubtract(map(to_uint8(1), 1, 2, 1), map(to_uint8(1), 1, 2, 1)) as res, to_type_name(res);
select mapSubtract(map(to_uint8(1), 1, 2, 1), map(to_uint8(1), 2, 2, 2)) as res, to_type_name(res); -- overflow
select mapSubtract(map(to_uint8(1), to_int32(1), 2, 1), map(to_uint8(1), to_int16(2), 2, 2)) as res, to_type_name(res);
select mapSubtract(map(1, to_float32(1.1), 2, 1), map(1, 2.2, 2, 1)) as res, to_type_name(res);
select mapSubtract(map(to_uint8(1), to_int32(1), 2, 1), map(to_uint8(1), to_int16(2), 2, 2)) as res, to_type_name(res);
select mapSubtract(map(to_uint8(3), to_int32(1)), map(to_uint8(1), to_int32(2), 2, 2)) as res, to_type_name(res);
