SET query_mode = 'table';
drop stream if exists t;

create stream t (a int) engine = MergeTree order by a;

-- some magic to satisfy conditions to run optimizations in MergeTreeRangeReader
insert into t select number < 20 ? 0 : 1 from numbers(50);
alter stream t add column s string default 'foo';

select s from t prewhere a != 1 where rand() % 2 = 0 limit 1;

drop stream t;
