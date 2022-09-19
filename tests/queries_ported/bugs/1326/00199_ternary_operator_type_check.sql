select (1 ? ('abc' as s) : 'def') = s;
select (1 ? to_fixed_string('abc' as s, 3) : 'def') = s;
select (1 ? to_fixed_string('abc' as s, 3) : to_fixed_string('def', 3)) = s;
select (1 ? ('abc' as s) : to_fixed_string('def', 3)) = s;

select (1 ? (today() as t) : yesterday()) = t;

select (1 ? (now() as n) : now() - 1) = n;

select (1 ? (to_uint8(0) as i) : to_uint8(1)) = i;
select (1 ? (to_uint16(0) as i) : to_uint8(1)) = i;
select (1 ? (to_uint32(0) as i) : to_uint8(1)) = i;
select (1 ? (to_uint64(0) as i) : to_uint8(1)) = i;
select (1 ? (to_int8(0) as i) : to_uint8(1)) = i;
select (1 ? (to_int16(0) as i) : to_uint8(1)) = i;
select (1 ? (to_int32(0) as i) : to_uint8(1)) = i;
select (1 ? (to_int64(0) as i) : to_uint8(1)) = i;

select (1 ? (to_uint8(0) as i) : to_uint16(1)) = i;
select (1 ? (to_uint16(0) as i) : to_uint16(1)) = i;
select (1 ? (to_uint32(0) as i) : to_uint16(1)) = i;
select (1 ? (to_uint64(0) as i) : to_uint16(1)) = i;
select (1 ? (to_int8(0) as i) : to_uint16(1)) = i;
select (1 ? (to_int16(0) as i) : to_uint16(1)) = i;
select (1 ? (to_int32(0) as i) : to_uint16(1)) = i;
select (1 ? (to_int64(0) as i) : to_uint16(1)) = i;

select (1 ? (to_uint8(0) as i) : to_uint32(1)) = i;
select (1 ? (to_uint16(0) as i) : to_uint32(1)) = i;
select (1 ? (to_uint32(0) as i) : to_uint32(1)) = i;
select (1 ? (to_uint64(0) as i) : to_uint32(1)) = i;
select (1 ? (to_int8(0) as i) : to_uint32(1)) = i;
select (1 ? (to_int16(0) as i) : to_uint32(1)) = i;
select (1 ? (to_int32(0) as i) : to_uint32(1)) = i;
select (1 ? (to_int64(0) as i) : to_uint32(1)) = i;

select (1 ? (to_uint8(0) as i) : to_uint64(1)) = i;
select (1 ? (to_uint16(0) as i) : to_uint64(1)) = i;
select (1 ? (to_uint32(0) as i) : to_uint64(1)) = i;
select (1 ? (to_uint64(0) as i) : to_uint64(1)) = i;
--select (1 ? (to_int8(0) as i) : to_uint64(1)) = i;
--select (1 ? (to_int16(0) as i) : to_uint64(1)) = i;
--select (1 ? (to_int32(0) as i) : to_uint64(1)) = i;
--select (1 ? (to_int64(0) as i) : to_uint64(1)) = i;

select (1 ? (to_uint8(0) as i) : to_int8(1)) = i;
select (1 ? (to_uint16(0) as i) : to_int8(1)) = i;
select (1 ? (to_uint32(0) as i) : to_int8(1)) = i;
--select (1 ? (to_uint64(0) as i) : to_int8(1)) = i;
select (1 ? (to_int8(0) as i) : to_int8(1)) = i;
select (1 ? (to_int16(0) as i) : to_int8(1)) = i;
select (1 ? (to_int32(0) as i) : to_int8(1)) = i;
select (1 ? (to_int64(0) as i) : to_int8(1)) = i;

select (1 ? (to_uint8(0) as i) : to_int16(1)) = i;
select (1 ? (to_uint16(0) as i) : to_int16(1)) = i;
select (1 ? (to_uint32(0) as i) : to_int16(1)) = i;
--select (1 ? (to_uint64(0) as i) : to_int16(1)) = i;
select (1 ? (to_int8(0) as i) : to_int16(1)) = i;
select (1 ? (to_int16(0) as i) : to_int16(1)) = i;
select (1 ? (to_int32(0) as i) : to_int16(1)) = i;
select (1 ? (to_int64(0) as i) : to_int16(1)) = i;

select (1 ? (to_uint8(0) as i) : to_int32(1)) = i;
select (1 ? (to_uint16(0) as i) : to_int32(1)) = i;
select (1 ? (to_uint32(0) as i) : to_int32(1)) = i;
--select (1 ? (to_uint64(0) as i) : to_int32(1)) = i;
select (1 ? (to_int8(0) as i) : to_int32(1)) = i;
select (1 ? (to_int16(0) as i) : to_int32(1)) = i;
select (1 ? (to_int32(0) as i) : to_int32(1)) = i;
select (1 ? (to_int64(0) as i) : to_int32(1)) = i;

select (1 ? (to_uint8(0) as i) : to_int64(1)) = i;
select (1 ? (to_uint16(0) as i) : to_int64(1)) = i;
select (1 ? (to_uint32(0) as i) : to_int64(1)) = i;
--select (1 ? (to_uint64(0) as i) : to_int64(1)) = i;
select (1 ? (to_int8(0) as i) : to_int64(1)) = i;
select (1 ? (to_int16(0) as i) : to_int64(1)) = i;
select (1 ? (to_int32(0) as i) : to_int64(1)) = i;
select (1 ? (to_int64(0) as i) : to_int64(1)) = i;
