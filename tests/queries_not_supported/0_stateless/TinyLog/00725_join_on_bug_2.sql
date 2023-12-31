set joined_subquery_requires_alias = 0;
SET query_mode = 'table';
drop stream if exists t_00725_2;
drop stream if exists s_00725_2;

create stream t_00725_2(a int64, b int64) engine = TinyLog;
insert into t_00725_2 values(1,1);
insert into t_00725_2 values(2,2);
create stream s_00725_2(a int64, b int64) engine = TinyLog;
insert into s_00725_2 values(1,1);

select a, b, s_a, s_b from t_00725_2 all left join (select a,b,a s_a, b s_b from s_00725_2) using (a,b);
select '-';
select t_00725_2.*, s_00725_2.* from t_00725_2 all left join s_00725_2 using (a,b);
select '-';
select a,b,s_a,s_b from t_00725_2 all left join (select a, b, a s_a, b s_b from s_00725_2) s_00725_2 on (s_00725_2.a = t_00725_2.a and s_00725_2.b = t_00725_2.b);
select '-';
select * from t_00725_2 all left join (select a s_a, b s_b from s_00725_2) on (s_a = t_00725_2.a and s_b = t_00725_2.b);
select '-';
select a,b,s_a,s_b from t_00725_2 all left join (select a,b, a s_a, b s_b from s_00725_2) on (s_a = t_00725_2.a and s_b = t_00725_2.b);
select '-';
select t_00725_2.*, s_00725_2.* from t_00725_2 all left join s_00725_2 on (s_00725_2.a = t_00725_2.a and s_00725_2.b = t_00725_2.b);

drop stream if exists t_00725_2;
drop stream if exists s_00725_2;
