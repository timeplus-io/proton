SET query_mode = 'table';
drop stream if exists t;
drop stream if exists t_buf;

create stream t (x uint64) engine = MergeTree order by (x, intHash64(x)) sample by intHash64(x);
insert into t select number from numbers(10000);
create stream t_buf as t engine = Buffer(currentDatabase(), 't', 16, 20, 100, 100000, 10000000, 50000000, 250000000);
insert into t_buf values (1);
select count() from t_buf sample 1/2 format Null;

drop stream if exists t_buf;
drop stream if exists t;
