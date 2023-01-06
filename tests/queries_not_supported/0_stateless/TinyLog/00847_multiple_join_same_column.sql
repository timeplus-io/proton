SET query_mode = 'table';
drop stream if exists t;
drop stream if exists s;
drop stream if exists y;

create stream t(a int64, b int64) engine = TinyLog;
create stream s(a int64, b int64) engine = TinyLog;
create stream y(a int64, b int64) engine = TinyLog;

insert into t values (1,1), (2,2);
insert into s values (1,1);
insert into y values (1,1);

select t.a, s.b, s.a, s.b, y.a, y.b from t
left join s on (t.a = s.a and t.b = s.b)
left join y on (y.a = s.a and y.b = s.b)
order by t.a
format Vertical;

select t.a, s.b, s.a, s.b, y.a, y.b from t
left join s on (t.a = s.a and s.b = t.b)
left join y on (y.a = s.a and y.b = s.b)
order by t.a
format PrettyCompactNoEscapes;

select t.a as t_a from t
left join s on s.a = t_a
order by t.a
format PrettyCompactNoEscapes;

select t.a, s.a as s_a from t
left join s on s.a = t.a
left join y on y.b = s.b
order by t.a
format PrettyCompactNoEscapes;

select t.a, t.a, t.b as t_b from t
left join s on t.a = s.a
left join y on y.b = s.b
order by t.a
format PrettyCompactNoEscapes;

select s.a, s.a, s.b as s_b, s.b from t
left join s on s.a = t.a
left join y on s.b = y.b
order by t.a
format PrettyCompactNoEscapes;

select y.a, y.a, y.b as y_b, y.b from t
left join s on s.a = t.a
left join y on y.b = s.b
order by t.a
format PrettyCompactNoEscapes;

select t.a, t.a as t_a, s.a, s.a as s_a, y.a, y.a as y_a from t
left join s on t.a = s.a
left join y on y.b = s.b
order by t.a
format PrettyCompactNoEscapes;

drop stream t;
drop stream s;
drop stream y;
