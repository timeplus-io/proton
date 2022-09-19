-- Tags: no-ordinary-database
SET query_mode = 'table';
drop stream if exists t;
drop stream if exists dist;
drop stream if exists buf;
drop stream if exists join;

select 'test flush on replace';
create stream t (n uint64, s string default 's' || to_string(n)) engine=Memory;
create stream dist (n int) engine=Distributed(test_shard_localhost, currentDatabase(), t);
create stream buf (n int) engine=Buffer(currentDatabase(), dist, 1, 10, 100, 10, 100, 1000, 1000);

system stop distributed sends dist;
insert into buf values (1);
replace table buf (n int) engine=Distributed(test_shard_localhost, currentDatabase(), dist);
replace table dist (n int) engine=Buffer(currentDatabase(), t, 1, 10, 100, 10, 100, 1000, 1000);

system stop distributed sends buf;
insert into buf values (2);
replace table buf (n int) engine=Buffer(currentDatabase(), dist, 1, 10, 100, 10, 100, 1000, 1000);
replace table dist (n int) engine=Distributed(test_shard_localhost, currentDatabase(), t);

system stop distributed sends dist;
insert into buf values (3);
replace table buf (n int) engine=Null;
replace table dist (n int) engine=Null;

select * from t order by n;

select 'exception on create and fill';
-- table is not created if select fails
create or replace table join engine=Join(ANY, INNER, n) as select * from t where throwIf(n); -- { serverError 395 }
select count() from system.tables where database=currentDatabase() and name='join';

-- table is created and filled
create or replace table join engine=Join(ANY, INNER, n) as select * from t;
select * from numbers(10) as t any join join on t.number=join.n order by n;

-- table is not replaced if select fails
insert into t(n) values (4);
replace table join engine=Join(ANY, INNER, n) as select * from t where throwIf(n); -- { serverError 395 }
select * from numbers(10) as t any join join on t.number=join.n order by n;

-- table is replaced
replace table join engine=Join(ANY, INNER, n) as select * from t;
select * from numbers(10) as t any join join on t.number=join.n order by n;

select name from system.tables where database=currentDatabase() order by name;

drop stream t;
drop stream dist;
drop stream buf;
drop stream join;
