SET query_mode = 'table';
drop stream if exists test_index;

create stream test_index(date date) engine MergeTree partition by to_YYYYMM(date) order by date;

insert into test_index values('2020-10-30');

select 1 from test_index where date < to_datetime('2020-10-30 06:00:00');

drop stream if exists test_index;

select to_type_name([-1, to_uint32(1)]);
-- We don't promote to wide integers
select to_type_name([-1, to_uint64(1)]); -- { serverError 386 }
select to_type_name([-1, to_int128(1)]);
select to_type_name([to_int64(-1), to_int128(1)]);
select to_type_name([to_uint64(1), toUInt256(1)]);
