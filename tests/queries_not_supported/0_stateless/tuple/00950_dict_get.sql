-- Tags: no-parallel

-- Must use `system` database and these tables - they're configured in tests/*_dictionary.xml
use system;
SET query_mode = 'table';
drop stream if exists ints;
drop stream if exists strings;
drop stream if exists decimals;

create stream ints (key uint64, i8 int8, i16 int16, i32 int32, i64 int64, u8 uint8, u16 uint16, u32 uint32, u64 uint64) Engine = Memory;
create stream strings (key uint64, str string) Engine = Memory;
create stream decimals (key uint64, d32 Decimal32(4), d64 Decimal64(6), d128 Decimal128(1)) Engine = Memory;

insert into ints values (1, 1, 1, 1, 1, 1, 1, 1, 1);
insert into strings values (1, '1');
insert into decimals values (1, 1, 1, 1);

select 'dictGet', 'flat_ints' as dict_name, to_uint64(1) as k,
    dictGet(dict_name, 'i8', k),
    dictGet(dict_name, 'i16', k),
    dictGet(dict_name, 'i32', k),
    dictGet(dict_name, 'i64', k),
    dictGet(dict_name, 'u8', k),
    dictGet(dict_name, 'u16', k),
    dictGet(dict_name, 'u32', k),
    dictGet(dict_name, 'u64', k),
    dictGet(dict_name, ('i8', 'i16', 'i32'), k);
select 'dictGetOrDefault', 'flat_ints' as dict_name, to_uint64(1) as k,
    dictGetOrDefault(dict_name, 'i8', k, to_int8(42)),
    dictGetOrDefault(dict_name, 'i16', k, to_int16(42)),
    dictGetOrDefault(dict_name, 'i32', k, to_int32(42)),
    dictGetOrDefault(dict_name, 'i64', k, to_int64(42)),
    dictGetOrDefault(dict_name, 'u8', k, to_uint8(42)),
    dictGetOrDefault(dict_name, 'u16', k, to_uint16(42)),
    dictGetOrDefault(dict_name, 'u32', k, to_uint32(42)),
    dictGetOrDefault(dict_name, 'u64', k, to_uint64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (to_int8(42), to_int16(42), to_int32(42)));
select 'dictGetOrDefault', 'flat_ints' as dict_name, to_uint64(0) as k,
    dictGetOrDefault(dict_name, 'i8', k, to_int8(42)),
    dictGetOrDefault(dict_name, 'i16', k, to_int16(42)),
    dictGetOrDefault(dict_name, 'i32', k, to_int32(42)),
    dictGetOrDefault(dict_name, 'i64', k, to_int64(42)),
    dictGetOrDefault(dict_name, 'u8', k, to_uint8(42)),
    dictGetOrDefault(dict_name, 'u16', k, to_uint16(42)),
    dictGetOrDefault(dict_name, 'u32', k, to_uint32(42)),
    dictGetOrDefault(dict_name, 'u64', k, to_uint64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (to_int8(42), to_int16(42), to_int32(42)));

select 'dictGet', 'hashed_ints' as dict_name, to_uint64(1) as k,
    dictGet(dict_name, 'i8', k),
    dictGet(dict_name, 'i16', k),
    dictGet(dict_name, 'i32', k),
    dictGet(dict_name, 'i64', k),
    dictGet(dict_name, 'u8', k),
    dictGet(dict_name, 'u16', k),
    dictGet(dict_name, 'u32', k),
    dictGet(dict_name, 'u64', k),
    dictGet(dict_name, ('i8', 'i16', 'i32'), k);
select 'dictGetOrDefault', 'hashed_ints' as dict_name, to_uint64(1) as k,
    dictGetOrDefault(dict_name, 'i8', k, to_int8(42)),
    dictGetOrDefault(dict_name, 'i16', k, to_int16(42)),
    dictGetOrDefault(dict_name, 'i32', k, to_int32(42)),
    dictGetOrDefault(dict_name, 'i64', k, to_int64(42)),
    dictGetOrDefault(dict_name, 'u8', k, to_uint8(42)),
    dictGetOrDefault(dict_name, 'u16', k, to_uint16(42)),
    dictGetOrDefault(dict_name, 'u32', k, to_uint32(42)),
    dictGetOrDefault(dict_name, 'u64', k, to_uint64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (to_int8(42), to_int16(42), to_int32(42)));
select 'dictGetOrDefault', 'hashed_ints' as dict_name, to_uint64(0) as k,
    dictGetOrDefault(dict_name, 'i8', k, to_int8(42)),
    dictGetOrDefault(dict_name, 'i16', k, to_int16(42)),
    dictGetOrDefault(dict_name, 'i32', k, to_int32(42)),
    dictGetOrDefault(dict_name, 'i64', k, to_int64(42)),
    dictGetOrDefault(dict_name, 'u8', k, to_uint8(42)),
    dictGetOrDefault(dict_name, 'u16', k, to_uint16(42)),
    dictGetOrDefault(dict_name, 'u32', k, to_uint32(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (to_int8(42), to_int16(42), to_int32(42)));

select 'dictGet', 'hashed_sparse_ints' as dict_name, to_uint64(1) as k,
    dictGet(dict_name, 'i8', k),
    dictGet(dict_name, 'i16', k),
    dictGet(dict_name, 'i32', k),
    dictGet(dict_name, 'i64', k),
    dictGet(dict_name, 'u8', k),
    dictGet(dict_name, 'u16', k),
    dictGet(dict_name, 'u32', k),
    dictGet(dict_name, 'u64', k),
    dictGet(dict_name, ('i8', 'i16', 'i32'), k);
select 'dictGetOrDefault', 'hashed_sparse_ints' as dict_name, to_uint64(1) as k,
    dictGetOrDefault(dict_name, 'i8', k, to_int8(42)),
    dictGetOrDefault(dict_name, 'i16', k, to_int16(42)),
    dictGetOrDefault(dict_name, 'i32', k, to_int32(42)),
    dictGetOrDefault(dict_name, 'i64', k, to_int64(42)),
    dictGetOrDefault(dict_name, 'u8', k, to_uint8(42)),
    dictGetOrDefault(dict_name, 'u16', k, to_uint16(42)),
    dictGetOrDefault(dict_name, 'u32', k, to_uint32(42)),
    dictGetOrDefault(dict_name, 'u64', k, to_uint64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (to_int8(42), to_int16(42), to_int32(42)));
select 'dictGetOrDefault', 'hashed_sparse_ints' as dict_name, to_uint64(0) as k,
    dictGetOrDefault(dict_name, 'i8', k, to_int8(42)),
    dictGetOrDefault(dict_name, 'i16', k, to_int16(42)),
    dictGetOrDefault(dict_name, 'i32', k, to_int32(42)),
    dictGetOrDefault(dict_name, 'i64', k, to_int64(42)),
    dictGetOrDefault(dict_name, 'u8', k, to_uint8(42)),
    dictGetOrDefault(dict_name, 'u16', k, to_uint16(42)),
    dictGetOrDefault(dict_name, 'u32', k, to_uint32(42)),
    dictGetOrDefault(dict_name, 'u64', k, to_uint64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (to_int8(42), to_int16(42), to_int32(42)));

select 'dictGet', 'cache_ints' as dict_name, to_uint64(1) as k,
    dictGet(dict_name, 'i8', k),
    dictGet(dict_name, 'i16', k),
    dictGet(dict_name, 'i32', k),
    dictGet(dict_name, 'i64', k),
    dictGet(dict_name, 'u8', k),
    dictGet(dict_name, 'u16', k),
    dictGet(dict_name, 'u32', k),
    dictGet(dict_name, 'u64', k),
    dictGet(dict_name, ('i8', 'i16', 'i32'), k);
select 'dictGetOrDefault', 'cache_ints' as dict_name, to_uint64(1) as k,
    dictGetOrDefault(dict_name, 'i8', k, to_int8(42)),
    dictGetOrDefault(dict_name, 'i16', k, to_int16(42)),
    dictGetOrDefault(dict_name, 'i32', k, to_int32(42)),
    dictGetOrDefault(dict_name, 'i64', k, to_int64(42)),
    dictGetOrDefault(dict_name, 'u8', k, to_uint8(42)),
    dictGetOrDefault(dict_name, 'u16', k, to_uint16(42)),
    dictGetOrDefault(dict_name, 'u32', k, to_uint32(42)),
    dictGetOrDefault(dict_name, 'u64', k, to_uint64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (to_int8(42), to_int16(42), to_int32(42)));
select 'dictGetOrDefault', 'cache_ints' as dict_name, to_uint64(0) as k,
    dictGetOrDefault(dict_name, 'i8', k, to_int8(42)),
    dictGetOrDefault(dict_name, 'i16', k, to_int16(42)),
    dictGetOrDefault(dict_name, 'i32', k, to_int32(42)),
    dictGetOrDefault(dict_name, 'i64', k, to_int64(42)),
    dictGetOrDefault(dict_name, 'u8', k, to_uint8(42)),
    dictGetOrDefault(dict_name, 'u16', k, to_uint16(42)),
    dictGetOrDefault(dict_name, 'u32', k, to_uint32(42)),
    dictGetOrDefault(dict_name, 'u64', k, to_uint64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (to_int8(42), to_int16(42), to_int32(42)));

select 'dictGet', 'complex_hashed_ints' as dict_name, tuple(to_uint64(1)) as k,
    dictGet(dict_name, 'i8', k),
    dictGet(dict_name, 'i16', k),
    dictGet(dict_name, 'i32', k),
    dictGet(dict_name, 'i64', k),
    dictGet(dict_name, 'u8', k),
    dictGet(dict_name, 'u16', k),
    dictGet(dict_name, 'u32', k),
    dictGet(dict_name, 'u64', k),
    dictGet(dict_name, ('i8', 'i16', 'i32'), k);
select 'dictGetOrDefault', 'complex_hashed_ints' as dict_name, tuple(to_uint64(1)) as k,
    dictGetOrDefault(dict_name, 'i8', k, to_int8(42)),
    dictGetOrDefault(dict_name, 'i16', k, to_int16(42)),
    dictGetOrDefault(dict_name, 'i32', k, to_int32(42)),
    dictGetOrDefault(dict_name, 'i64', k, to_int64(42)),
    dictGetOrDefault(dict_name, 'u8', k, to_uint8(42)),
    dictGetOrDefault(dict_name, 'u16', k, to_uint16(42)),
    dictGetOrDefault(dict_name, 'u32', k, to_uint32(42)),
    dictGetOrDefault(dict_name, 'u64', k, to_uint64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (to_int8(42), to_int16(42), to_int32(42)));
select 'dictGetOrDefault', 'complex_hashed_ints' as dict_name, tuple(to_uint64(0)) as k,
    dictGetOrDefault(dict_name, 'i8', k, to_int8(42)),
    dictGetOrDefault(dict_name, 'i16', k, to_int16(42)),
    dictGetOrDefault(dict_name, 'i32', k, to_int32(42)),
    dictGetOrDefault(dict_name, 'i64', k, to_int64(42)),
    dictGetOrDefault(dict_name, 'u8', k, to_uint8(42)),
    dictGetOrDefault(dict_name, 'u16', k, to_uint16(42)),
    dictGetOrDefault(dict_name, 'u32', k, to_uint32(42)),
    dictGetOrDefault(dict_name, 'u64', k, to_uint64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (to_int8(42), to_int16(42), to_int32(42)));

select 'dictGet', 'complex_cache_ints' as dict_name, tuple(to_uint64(1)) as k,
    dictGet(dict_name, 'i8', k),
    dictGet(dict_name, 'i16', k),
    dictGet(dict_name, 'i32', k),
    dictGet(dict_name, 'i64', k),
    dictGet(dict_name, 'u8', k),
    dictGet(dict_name, 'u16', k),
    dictGet(dict_name, 'u32', k),
    dictGet(dict_name, 'u64', k),
    dictGet(dict_name, ('i8', 'i16', 'i32'), k);;
select 'dictGetOrDefault', 'complex_cache_ints' as dict_name, tuple(to_uint64(1)) as k,
    dictGetOrDefault(dict_name, 'i8', k, to_int8(42)),
    dictGetOrDefault(dict_name, 'i16', k, to_int16(42)),
    dictGetOrDefault(dict_name, 'i32', k, to_int32(42)),
    dictGetOrDefault(dict_name, 'i64', k, to_int64(42)),
    dictGetOrDefault(dict_name, 'u8', k, to_uint8(42)),
    dictGetOrDefault(dict_name, 'u16', k, to_uint16(42)),
    dictGetOrDefault(dict_name, 'u32', k, to_uint32(42)),
    dictGetOrDefault(dict_name, 'u64', k, to_uint64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (to_int8(42), to_int16(42), to_int32(42)));
select 'dictGetOrDefault', 'complex_cache_ints' as dict_name, tuple(to_uint64(0)) as k,
    dictGetOrDefault(dict_name, 'i8', k, to_int8(42)),
    dictGetOrDefault(dict_name, 'i16', k, to_int16(42)),
    dictGetOrDefault(dict_name, 'i32', k, to_int32(42)),
    dictGetOrDefault(dict_name, 'i64', k, to_int64(42)),
    dictGetOrDefault(dict_name, 'u8', k, to_uint8(42)),
    dictGetOrDefault(dict_name, 'u16', k, to_uint16(42)),
    dictGetOrDefault(dict_name, 'u32', k, to_uint32(42)),
    dictGetOrDefault(dict_name, 'u64', k, to_uint64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (to_int8(42), to_int16(42), to_int32(42)));

--

select 'dictGet', 'flat_strings' as dict_name, to_uint64(1) as k, dictGet(dict_name, 'str', k), dictGet(dict_name, ('str'), k);
select 'dictGetOrDefault', 'flat_strings' as dict_name, to_uint64(1) as k, dictGetOrDefault(dict_name, 'str', k, '*'), dictGetOrDefault(dict_name, ('str'), k, ('*'));
select 'dictGetOrDefault', 'flat_strings' as dict_name, to_uint64(0) as k, dictGetOrDefault(dict_name, 'str', k, '*'), dictGetOrDefault(dict_name, ('str'), k, ('*'));

select 'dictGet', 'hashed_strings' as dict_name, to_uint64(1) as k, dictGet(dict_name, 'str', k), dictGet(dict_name, ('str'), k);
select 'dictGetOrDefault', 'hashed_strings' as dict_name, to_uint64(1) as k, dictGetOrDefault(dict_name, 'str', k, '*'), dictGetOrDefault(dict_name, ('str'), k, ('*'));
select 'dictGetOrDefault', 'hashed_strings' as dict_name, to_uint64(0) as k, dictGetOrDefault(dict_name, 'str', k, '*'), dictGetOrDefault(dict_name, ('str'), k, ('*'));

select 'dictGet', 'cache_strings' as dict_name, to_uint64(1) as k, dictGet(dict_name, 'str', k), dictGet(dict_name, ('str'), k);
select 'dictGetOrDefault', 'cache_strings' as dict_name, to_uint64(1) as k, dictGetOrDefault(dict_name, 'str', k, '*'), dictGetOrDefault(dict_name, ('str'), k, ('*'));
select 'dictGetOrDefault', 'cache_strings' as dict_name, to_uint64(0) as k, dictGetOrDefault(dict_name, 'str', k, '*'), dictGetOrDefault(dict_name, ('str'), k, ('*'));

select 'dictGet', 'complex_hashed_strings' as dict_name, to_uint64(1) as k, dictGet(dict_name, 'str', tuple(k)), dictGet(dict_name, ('str'), tuple(k));
select 'dictGetOrDefault', 'complex_hashed_strings' as dict_name, to_uint64(1) as k, dictGetOrDefault(dict_name, 'str', tuple(k), '*'), dictGetOrDefault(dict_name, ('str'), tuple(k), ('*'));
select 'dictGetOrDefault', 'complex_hashed_strings' as dict_name, to_uint64(0) as k, dictGetOrDefault(dict_name, 'str', tuple(k), '*'), dictGetOrDefault(dict_name, ('str'), tuple(k), ('*'));

select 'dictGet', 'complex_cache_strings' as dict_name, to_uint64(1) as k, dictGet(dict_name, 'str', tuple(k)), dictGet(dict_name, ('str'), tuple(k));
select 'dictGetOrDefault', 'complex_cache_strings' as dict_name, to_uint64(1) as k, dictGetOrDefault(dict_name, 'str', tuple(k), '*'), dictGetOrDefault(dict_name, ('str'), tuple(k), ('*'));
select 'dictGetOrDefault', 'complex_cache_strings' as dict_name, to_uint64(0) as k, dictGetOrDefault(dict_name, 'str', tuple(k), '*'), dictGetOrDefault(dict_name, ('str'), tuple(k), ('*'));

--

select 'dictGet', 'flat_decimals' as dict_name, to_uint64(1) as k,
    dictGet(dict_name, 'd32', k),
    dictGet(dict_name, 'd64', k),
    dictGet(dict_name, 'd128', k),
    dictGet(dict_name, ('d32', 'd64', 'd128'), k);
select 'dictGetOrDefault', 'flat_decimals' as dict_name, to_uint64(1) as k,
    dictGetOrDefault(dict_name, 'd32', k, to_decimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, to_decimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (to_decimal32(42, 4), to_decimal64(42, 6), toDecimal128(42, 1)));
select 'dictGetOrDefault', 'flat_decimals' as dict_name, to_uint64(0) as k,
    dictGetOrDefault(dict_name, 'd32', k, to_decimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, to_decimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (to_decimal32(42, 4), to_decimal64(42, 6), toDecimal128(42, 1)));

select 'dictGet', 'hashed_decimals' as dict_name, to_uint64(1) as k,
    dictGet(dict_name, 'd32', k),
    dictGet(dict_name, 'd64', k),
    dictGet(dict_name, 'd128', k),
    dictGet(dict_name, ('d32', 'd64', 'd128'), k);
select 'dictGetOrDefault', 'hashed_decimals' as dict_name, to_uint64(1) as k,
    dictGetOrDefault(dict_name, 'd32', k, to_decimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, to_decimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (to_decimal32(42, 4), to_decimal64(42, 6), toDecimal128(42, 1)));
select 'dictGetOrDefault', 'hashed_decimals' as dict_name, to_uint64(0) as k,
    dictGetOrDefault(dict_name, 'd32', k, to_decimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, to_decimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (to_decimal32(42, 4), to_decimal64(42, 6), toDecimal128(42, 1)));

select 'dictGet', 'cache_decimals' as dict_name, to_uint64(1) as k,
    dictGet(dict_name, 'd32', k),
    dictGet(dict_name, 'd64', k),
    dictGet(dict_name, 'd128', k),
    dictGet(dict_name, ('d32', 'd64', 'd128'), k);
select 'dictGetOrDefault', 'cache_decimals' as dict_name, to_uint64(1) as k,
    dictGetOrDefault(dict_name, 'd32', k, to_decimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, to_decimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (to_decimal32(42, 4), to_decimal64(42, 6), toDecimal128(42, 1)));
select 'dictGetOrDefault', 'cache_decimals' as dict_name, to_uint64(0) as k,
    dictGetOrDefault(dict_name, 'd32', k, to_decimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, to_decimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (to_decimal32(42, 4), to_decimal64(42, 6), toDecimal128(42, 1)));

select 'dictGet', 'complex_hashed_decimals' as dict_name, tuple(to_uint64(1)) as k,
    dictGet(dict_name, 'd32', k),
    dictGet(dict_name, 'd64', k),
    dictGet(dict_name, 'd128', k),
    dictGet(dict_name, ('d32', 'd64', 'd128'), k);
select 'dictGetOrDefault', 'complex_hashed_decimals' as dict_name, tuple(to_uint64(1)) as k,
    dictGetOrDefault(dict_name, 'd32', k, to_decimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, to_decimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (to_decimal32(42, 4), to_decimal64(42, 6), toDecimal128(42, 1)));
select 'dictGetOrDefault', 'complex_hashed_decimals' as dict_name, tuple(to_uint64(0)) as k,
    dictGetOrDefault(dict_name, 'd32', k, to_decimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, to_decimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (to_decimal32(42, 4), to_decimal64(42, 6), toDecimal128(42, 1)));

select 'dictGet', 'complex_cache_decimals' as dict_name, tuple(to_uint64(1)) as k,
    dictGet(dict_name, 'd32', k),
    dictGet(dict_name, 'd64', k),
    dictGet(dict_name, 'd128', k),
    dictGet(dict_name, ('d32', 'd64', 'd128'), k);
select 'dictGetOrDefault', 'complex_cache_decimals' as dict_name, tuple(to_uint64(1)) as k,
    dictGetOrDefault(dict_name, 'd32', k, to_decimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, to_decimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (to_decimal32(42, 4), to_decimal64(42, 6), toDecimal128(42, 1)));
select 'dictGetOrDefault', 'complex_cache_decimals' as dict_name, tuple(to_uint64(0)) as k,
    dictGetOrDefault(dict_name, 'd32', k, to_decimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, to_decimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (to_decimal32(42, 4), to_decimal64(42, 6), toDecimal128(42, 1)));

--
-- Keep the tables, so that the dictionaries can be reloaded correctly and
-- SYSTEM RELOAD DICTIONARIES doesn't break.
-- We could also:
-- * drop the dictionaries -- not possible, they are configured in a .xml;
-- * switch dictionaries to DDL syntax so that they can be dropped -- tedious,
--   because there are a couple dozens of them, and also we need to have some
--   .xml dictionaries in tests so that we test backward compatibility with this
--   format;
-- * unload dictionaries -- no command for that.
--
