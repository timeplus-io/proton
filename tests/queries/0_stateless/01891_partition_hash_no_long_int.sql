-- Tags: long
SET query_mode = 'table';
drop stream if exists tab;
create stream tab (i8 int8, i16 Int16, i32 int32, i64 int64, u8 uint8, u16 uint16, u32 uint32, u64 uint64, id UUID, s string, fs FixedString(33), a array(uint8), t tuple(uint16, uint32), d date, dt datetime('Europe/Moscow'), dt64 DateTime64(3, 'Europe/Moscow'), dec128 Decimal128(3), lc LowCardinality(string)) engine = MergeTree PARTITION BY (i8, i16, i32, i64, u8, u16, u32, u64, id, s, fs, a, t, d, dt, dt64, dec128, lc) order by tuple();
insert into tab values (-1, -1, -1, -1, -1, -1, -1, -1, '61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'a', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', [1, 2, 3], (-1, -2), '2020-01-01', '2020-01-01 01:01:01', '2020-01-01 01:01:01', '123.456', 'a');
-- Here we check that partition id did not change.
-- Different result means Backward Incompatible Change. Old partitions will not be accepted by new server.
select partition_id from system.parts where table = 'tab' and database = currentDatabase();
drop stream if exists tab;
