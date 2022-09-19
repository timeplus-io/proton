-- Tags: no-ordinary-database, no-parallel

drop stream if exists t1;

replace table t1 (n uint64, s string) engine=MergeTree order by n; -- { serverError 60 }
show tables;
create or replace table t1 (n uint64, s string) engine=MergeTree order by n;
show tables;
show create stream t1;

insert into t1 values (1, 'test');
create or replace table t1 (n uint64, s Nullable(string)) engine=MergeTree order by n;
insert into t1 values (2, null);
show tables;
show create stream t1;
select * from t1;

replace table t1 (n uint64) engine=MergeTree order by n;
insert into t1 values (3);
show tables;
show create stream t1;
select * from t1;

drop stream t1;
