-- TODO: set special_sort = 'opencl_bitonic';

select to_uint8(number * 2) as x from numbers(42) order by x desc;
select to_int8(number * 2) as x from numbers(42) order by x desc;
select to_uint16(number * 2) as x from numbers(42) order by x desc;
select to_int16(number * 2) as x from numbers(42) order by x desc;
select to_uint32(number * 2) as x from numbers(42) order by x desc;
select to_int32(number * 2) as x from numbers(42) order by x desc;
select to_uint64(number * 2) as x from numbers(42) order by x desc;
select to_int64(number * 2) as x from numbers(42) order by x desc;
