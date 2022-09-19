SET query_mode = 'table';
drop stream if exists t_00725_4;
drop stream if exists s_00725_4;

create stream t_00725_4(a int64, b int64, c string) engine = TinyLog;
insert into t_00725_4 values(1,1,'a'),(2,2,'b');
create stream s_00725_4(a int64, b int64, c string) engine = TinyLog;
insert into s_00725_4 values(1,1,'a');


select t_00725_4.* from t_00725_4 all left join s_00725_4 on (s_00725_4.a = t_00725_4.a and s_00725_4.b = t_00725_4.b) where s_00725_4.a = 0 and s_00725_4.b = 0;

drop stream if exists t_00725_4;
drop stream if exists s_00725_4;
