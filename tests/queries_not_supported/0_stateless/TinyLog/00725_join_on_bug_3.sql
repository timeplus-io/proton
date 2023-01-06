SET query_mode = 'table';
drop stream if exists t_00725_3;
drop stream if exists z_00725_3;

create stream t_00725_3(a int64, b int64) engine = TinyLog;
insert into t_00725_3 values(1,1);
insert into t_00725_3 values(2,2);
create stream z_00725_3(c int64, d int64, e int64) engine = TinyLog;
insert into z_00725_3 values(1,1,1);

select * from t_00725_3 all left join z_00725_3 on (z_00725_3.c = t_00725_3.a and z_00725_3.d = t_00725_3.b);

drop stream if exists t_00725_3;
drop stream if exists z_00725_3;

