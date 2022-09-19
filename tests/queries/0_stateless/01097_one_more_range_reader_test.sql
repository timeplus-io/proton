SET query_mode = 'table';
drop stream if exists t;

create stream t (id uint32, a int) engine = MergeTree order by id;

insert into t values (1, 0) (2, 1) (3, 0) (4, 0) (5, 0);
alter stream t add column s string default 'foo';
select s from t prewhere a = 1;

drop stream t;

create stream t (id uint32, a int) engine = MergeTree order by id;

insert into t values (1, 1) (2, 1) (3, 0) (4, 0) (5, 0);
alter stream t add column s string default 'foo';
select s from t prewhere a = 1;

drop stream t;
