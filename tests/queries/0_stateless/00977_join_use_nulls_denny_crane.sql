SET query_mode = 'table';
drop stream if exists t;
drop stream if exists s;

create stream t(a int64, b int64, c string) engine = Memory;
create stream s(a int64, b int64, c string) engine = Memory;

insert into t values(1,1,'a');
insert into s values(2,2,'a');

select t.*, s.a, s.b, s.c from t left join s on (s.a = t.a and s.b = t.b);
select t.*, s.a, s.b, s.c from t right join s on (s.a = t.a and s.b = t.b);
select t.*, s.a, s.b, s.c from t left join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;
select t.*, s.a, s.b, s.c from t right join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;

drop stream t;
drop stream s;

create stream t(a int64, b int64, c Nullable(string)) engine = Memory;
create stream s(a int64, b int64, c Nullable(string)) engine = Memory;

insert into t values(1,1,'a');
insert into s values(2,2,'a');

select * from t left join s on (s.a = t.a and s.b = t.b);
select * from t right join s on (s.a = t.a and s.b = t.b);
select t.*, s.* from t left join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;
select t.*, s.* from t right join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;

drop stream t;
drop stream s;

create stream t(a int64, b Nullable(int64), c string) engine = Memory;
create stream s(a int64, b Nullable(int64), c string) engine = Memory;

insert into t values(1,1,'a');
insert into s values(2,2,'a');

select t.*, s.* from t left join s on (s.a = t.a and s.b = t.b);
select t.*, s.* from t right join s on (s.a = t.a and s.b = t.b);
select * from t left join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;
select * from t right join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;

drop stream t;
drop stream s;

create stream t(a int64, b Nullable(int64), c Nullable(string)) engine = Memory;
create stream s(a int64, b Nullable(int64), c Nullable(string)) engine = Memory;

insert into t values(1,1,'a');
insert into s values(2,2,'a');

select t.*, s.a, s.b, s.c from t left join s on (s.a = t.a and s.b = t.b);
select t.*, s.a, s.b, s.c from t right join s on (s.a = t.a and s.b = t.b);
select * from t left join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;
select * from t right join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;

drop stream t;
drop stream s;

create stream t(a Nullable(int64), b Nullable(int64), c Nullable(string)) engine = Memory;
create stream s(a Nullable(int64), b Nullable(int64), c Nullable(string)) engine = Memory;

insert into t values(1,1,'a');
insert into s values(2,2,'a');

select * from t left join s on (s.a = t.a and s.b = t.b);
select * from t right join s on (s.a = t.a and s.b = t.b);
select t.*, s.a, s.b, s.c from t left join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;
select t.*, s.a, s.b, s.c from t right join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;

drop stream t;
drop stream s;
