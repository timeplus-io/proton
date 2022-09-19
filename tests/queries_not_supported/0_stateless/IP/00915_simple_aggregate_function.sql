set optimize_throw_if_noop = 1;

-- basic test
SET query_mode = 'table';
drop stream if exists simple;

create stream simple (id uint64,val SimpleAggregateFunction(sum,Double)) engine=AggregatingMergeTree order by id;
insert into simple select number,number from system.numbers limit 10;

select * from simple;
select * from simple final order by id;
select to_type_name(val) from simple limit 1;

-- merge
insert into simple select number,number from system.numbers limit 10;

select * from simple final order by id;

optimize table simple final;
select * from simple;

-- complex types
drop stream if exists simple;

create stream simple (
    id uint64,
    nullable_str SimpleAggregateFunction(anyLast,Nullable(string)),
    low_str SimpleAggregateFunction(anyLast,LowCardinality(Nullable(string))),
    ip SimpleAggregateFunction(anyLast,IPv4),
    status SimpleAggregateFunction(groupBitOr, uint32),
    tup SimpleAggregateFunction(sumMap, tuple(array(int32), array(int64))),
    tup_min SimpleAggregateFunction(minMap, tuple(array(int32), array(int64))),
    tup_max SimpleAggregateFunction(maxMap, tuple(array(int32), array(int64))),
    arr SimpleAggregateFunction(groupArrayArray, array(int32)),
    uniq_arr SimpleAggregateFunction(groupUniqArrayArray, array(int32))
) engine=AggregatingMergeTree order by id;
insert into simple values(1,'1','1','1.1.1.1', 1, ([1,2], [1,1]), ([1,2], [1,1]), ([1,2], [1,1]), [1,2], [1,2]);
insert into simple values(1,null,'2','2.2.2.2', 2, ([1,3], [1,1]), ([1,3], [2,2]), ([1,3], [2,2]), [2,3,4], [2,3,4]);
-- string longer then MAX_SMALL_STRING_SIZE (actual string length is 100)
insert into simple values(10,'10','10','10.10.10.10', 4, ([2,3], [1,1]), ([2,3], [3,3]), ([2,3], [3,3]), [], []);
insert into simple values(10,'2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222','20','20.20.20.20', 1, ([2, 4], [1,1]), ([2, 4], [4,4]), ([2, 4], [4,4]), [], []);

select * from simple final order by id;
select to_type_name(nullable_str),to_type_name(low_str),to_type_name(ip),to_type_name(status), to_type_name(tup), to_type_name(tup_min), to_type_name(tup_max), to_type_name(arr), to_type_name(uniq_arr) from simple limit 1;

optimize table simple final;

drop stream simple;

drop stream if exists with_overflow;
create stream with_overflow (
    id uint64,
    s SimpleAggregateFunction(sumWithOverflow, uint8)
) engine AggregatingMergeTree order by id;

insert into with_overflow select 1, 1 from numbers(256);

optimize table with_overflow final;

select 'with_overflow', * from with_overflow;
drop stream with_overflow;
