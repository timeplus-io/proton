-- Tags: shard

set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=2;

create temporary table data (id uint64) engine=Memory() as with [
    0,
    1,
    0x7f, 0x80, 0xff,
    0x7fff, 0x8000, 0xffff,
    0x7fffffff, 0x80000000, 0xffffffff,
    0x7fffffffffffffff, 0x8000000000000000, 0xffffffffffffffff
] as values select array_join(values) id;

-- { echoOn }

-- int8, int8
select _shard_num, * from remote('127.{1..4}', view(select to_int8(id) id from data), to_int8(id)) where id in (0, 1, 0x7f) order by _shard_num, id;
-- int8, uint8
select _shard_num, * from remote('127.{1..4}', view(select to_int8(id) id from data), to_uint8(id)) where id in (0, 1, 0x7f) order by _shard_num, id;
-- uint8, uint8
select _shard_num, * from remote('127.{1..4}', view(select to_uint8(id) id from data), to_uint8(id)) where id in (0, 1, 0x7f, 0x80, 0xff) order by _shard_num, id;
-- uint8, int8
select _shard_num, * from remote('127.{1..4}', view(select to_uint8(id) id from data), to_int8(id)) where id in (0, 1, 0x7f, 0x80, 0xff) order by _shard_num, id;

-- Int16, Int16
select _shard_num, * from remote('127.{1..4}', view(select to_int16(id) id from data), to_int16(id)) where id in (0, 1, 0x7fff) order by _shard_num, id;
-- Int16, uint16
select _shard_num, * from remote('127.{1..4}', view(select to_int16(id) id from data), to_uint16(id)) where id in (0, 1, 0x7fff) order by _shard_num, id;
-- uint16, uint16
select _shard_num, * from remote('127.{1..4}', view(select to_uint16(id) id from data), to_uint16(id)) where id in (0, 1, 0x7fff, 0x8000, 0xffff) order by _shard_num, id;
-- uint16, Int16
select _shard_num, * from remote('127.{1..4}', view(select to_uint16(id) id from data), to_int16(id)) where id in (0, 1, 0x7fff, 0x8000, 0xffff) order by _shard_num, id;

-- int32, int32
select _shard_num, * from remote('127.{1..4}', view(select to_int32(id) id from data), to_int32(id)) where id in (0, 1, 0x7fffffff) order by _shard_num, id;
-- int32, uint32
select _shard_num, * from remote('127.{1..4}', view(select to_int32(id) id from data), to_uint32(id)) where id in (0, 1, 0x7fffffff) order by _shard_num, id;
-- uint32, uint32
select _shard_num, * from remote('127.{1..4}', view(select to_uint32(id) id from data), to_uint32(id)) where id in (0, 1, 0x7fffffff, 0x80000000, 0xffffffff) order by _shard_num, id;
-- uint32, int32
select _shard_num, * from remote('127.{1..4}', view(select to_uint32(id) id from data), to_int32(id)) where id in (0, 1, 0x7fffffff, 0x80000000, 0xffffffff) order by _shard_num, id;

-- int64, int64
select _shard_num, * from remote('127.{1..4}', view(select to_int64(id) id from data), to_int64(id)) where id in (0, 1, 0x7fffffffffffffff) order by _shard_num, id;
-- int64, uint64
select _shard_num, * from remote('127.{1..4}', view(select to_int64(id) id from data), to_uint64(id)) where id in (0, 1, 0x7fffffffffffffff) order by _shard_num, id;
-- uint64, uint64
select _shard_num, * from remote('127.{1..4}', view(select to_uint64(id) id from data), to_uint64(id)) where id in (0, 1, 0x7fffffffffffffff, 0x8000000000000000, 0xffffffffffffffff) order by _shard_num, id;
-- uint64, int64
select _shard_num, * from remote('127.{1..4}', view(select to_uint64(id) id from data), to_int64(id)) where id in (0, 1, 0x7fffffffffffffff, 0x8000000000000000, 0xffffffffffffffff) order by _shard_num, id;

-- modulo(int8)
select distinct _shard_num, * from remote('127.{1..4}', view(select to_int16(id) id from data), to_int8(id)%255) where id in (-1) order by _shard_num, id;
-- modulo(uint8)
select distinct _shard_num, * from remote('127.{1..4}', view(select to_int16(id) id from data), to_uint8(id)%255) where id in (-1) order by _shard_num, id;

-- { echoOff }

-- those two had been reported initially by amosbird:
-- (the problem is that murmurHash3_32() returns different value to to_int64(1) and to_uint64(1))
---- error for local node
select * from remote('127.{1..4}', view(select number id from numbers(0)), bitAnd(murmurHash3_32(id), 2147483647)) where id in (2, 3);
---- error for remote node
select * from remote('127.{1..8}', view(select number id from numbers(0)), bitAnd(murmurHash3_32(id), 2147483647)) where id in (2, 3);
