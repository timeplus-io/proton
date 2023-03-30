drop stream if exists t;
drop stream if exists s;
drop stream if exists y;

create stream t(a int64, b int64) engine = Memory;
create stream s(a int64, b int64) engine = Memory;
create stream y(a int64, b int64) engine = Memory;

insert into t values (1,1), (2,2);
insert into s values (1,1);
insert into y values (1,1);

select s.a, s.a, s.b as s_b, s.b from t
left join s on s.a = t.a
left join y on s.b = y.b
order by t.a, s.a, s.b;

select max(s.a) from t
left join s on s.a = t.a
left join y on s.b = y.b
group by t.a order by t.a;

select t.a, t.a as t_a, s.a, s.a as s_a, y.a, y.a as y_a from t
left join s on t.a = s.a
left join y on y.b = s.b
order by t.a, s.a, y.a;

select t.a, t.a as t_a, max(s.a) from t
left join s on t.a = s.a
left join y on y.b = s.b
group by t.a order by t.a;

drop stream t;
drop stream s;
drop stream y;
