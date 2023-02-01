-- Tags: no-ordinary-database, no-parallel

drop stream if exists t1;

replace stream t1 (n uint64, s string) engine=MergeTree order by n; -- { serverError 60 }
show streams;
create or replace stream t1 (n uint64, s string) engine=MergeTree order by n;
show streams;
show create stream t1;

insert into t1 values (1, 'test');
create or replace stream t1 (n uint64, s nullable(string)) engine=MergeTree order by n;
insert into t1 values (2, null);
show streams;
show create stream t1;
select * from t1;

replace stream t1 (n uint64) engine=MergeTree order by n;
insert into t1 values (3);
show streams;
show create stream t1;
select * from t1;

drop stream t1;
