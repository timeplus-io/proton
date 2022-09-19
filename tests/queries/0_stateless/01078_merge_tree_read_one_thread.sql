SET query_mode = 'table';
drop stream if exists t;

create stream t (a int, b int) engine = MergeTree order by (a, b) settings index_granularity = 400;

insert into t select 0, 0 from numbers(50);
insert into t select 0, 1  from numbers(350);
insert into t select 1, 2  from numbers(400);
insert into t select 2, 2  from numbers(400);
insert into t select 3, 0 from numbers(100);

select sleep(1) format Null; -- sleep a bit to wait possible merges after insert

set max_threads = 1;
optimize table t final;

select sum(a) from t where a in (0, 3) and b = 0;

drop stream t;
