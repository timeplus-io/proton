create temporary table t1 (a Nullable(uint8));
insert into t1 values (2.4);
select * from t1;

create temporary table t2 (a uint8);
insert into t2 values (2.4);
select * from t2;
