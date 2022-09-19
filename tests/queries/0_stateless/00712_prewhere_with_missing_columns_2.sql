SET query_mode = 'table';
drop stream if exists t_00712_1;
create stream t_00712_1 (a int32, b int32) engine = MergeTree partition by (a,b) order by (a);

insert into t_00712_1 values (1, 1);
alter stream t_00712_1 add column c int32;

select b from t_00712_1 prewhere a < 1000;
select c from t_00712_1 where a < 1000;
select c from t_00712_1 prewhere a < 1000;

drop stream t_00712_1;

