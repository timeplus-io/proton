-- Tags: no-parallel

drop stream if exists test_01054_overflow_ints;

create stream test_01054_overflow_ints (key uint64, i8 int8, i16 int16, i32 int32, i64 int64, u8 uint8, u16 uint16, u32 uint32, u64 uint64) Engine = Memory;

insert into test_01054_overflow_ints values (1, 1, 1, 1, 1, 1, 1, 1, 1);
insert into test_01054_overflow_ints values (2, 2, 2, 2, 2, 2, 2, 2, 2);
insert into test_01054_overflow_ints values (3, 3, 3, 3, 3, 3, 3, 3, 3);
insert into test_01054_overflow_ints values (4, 4, 4, 4, 4, 4, 4, 4, 4);
insert into test_01054_overflow_ints values (5, 5, 5, 5, 5, 5, 5, 5, 5);
insert into test_01054_overflow_ints values (6, 6, 6, 6, 6, 6, 6, 6, 6);
insert into test_01054_overflow_ints values (7, 7, 7, 7, 7, 7, 7, 7, 7);
insert into test_01054_overflow_ints values (8, 8, 8, 8, 8, 8, 8, 8, 8);
insert into test_01054_overflow_ints values (9, 9, 9, 9, 9, 9, 9, 9, 9);
insert into test_01054_overflow_ints values (10, 10, 10, 10, 10, 10, 10, 10, 10);
insert into test_01054_overflow_ints values (11, 11, 11, 11, 11, 11, 11, 11, 11);
insert into test_01054_overflow_ints values (12, 12, 12, 12, 12, 12, 12, 12, 12);
insert into test_01054_overflow_ints values (13, 13, 13, 13, 13, 13, 13, 13, 13);
insert into test_01054_overflow_ints values (14, 14, 14, 14, 14, 14, 14, 14, 14);
insert into test_01054_overflow_ints values (15, 15, 15, 15, 15, 15, 15, 15, 15);
insert into test_01054_overflow_ints values (16, 16, 16, 16, 16, 16, 16, 16, 16);
insert into test_01054_overflow_ints values (17, 17, 17, 17, 17, 17, 17, 17, 17);
insert into test_01054_overflow_ints values (18, 18, 18, 18, 18, 18, 18, 18, 18);
insert into test_01054_overflow_ints values (19, 19, 19, 19, 19, 19, 19, 19, 19);
insert into test_01054_overflow_ints values (20, 20, 20, 20, 20, 20, 20, 20, 20);

select 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(1)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(2)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(3)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(4)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(5)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(6)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(7)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(8)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(9)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(10)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(11)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(12)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(13)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(14)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(15)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(16)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(17)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(18)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(19)), 
dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(20));

SELECT arrayMap(x -> dict_get('one_cell_cache_ints_overflow', 'i8', to_uint64(x)), array)
FROM
(
    SELECT [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20] AS array
);

DROP STREAM if exists test_01054_overflow_ints;

