drop stream if exists t;

create stream t (i int, j int, k int, projection p (select * order by j)) engine MergeTree order by i settings index_granularity = 1;

insert into t select number, number, number from numbers(10);

set allow_experimental_projection_optimization = 1, max_rows_to_read = 3;

select * from t where i < 5 and j in (1, 2);

drop stream t;

drop stream if exists test;

create stream test (name string, time int64) engine MergeTree order by time;

insert into test values ('hello world', 1662336000241);

select count() from (select from_unix_timestamp64_milli(time, 'UTC') as time_fmt, name from test where time_fmt > '2022-09-05 00:00:00');

drop stream test;
