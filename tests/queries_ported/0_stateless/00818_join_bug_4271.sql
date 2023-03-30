drop stream if exists t_00818;
drop stream if exists s_00818;

create stream t_00818(a nullable(int64), b nullable(int64), c nullable(string)) engine = Memory;
create stream s_00818(a nullable(int64), b nullable(int64), c nullable(string)) engine = Memory;

insert into t_00818 values(1,1,'a'), (2,2,'b');
insert into s_00818 values(1,1,'a');

select * from t_00818 left join s_00818 on t_00818.a = s_00818.a ORDER BY t_00818.a;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a and t_00818.a = s_00818.b ORDER BY t_00818.a;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a where s_00818.a = 1 ORDER BY t_00818.a;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a and t_00818.a = s_00818.a ORDER BY t_00818.a;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a and t_00818.b = s_00818.a ORDER BY t_00818.a;

drop stream t_00818;
drop stream s_00818;
