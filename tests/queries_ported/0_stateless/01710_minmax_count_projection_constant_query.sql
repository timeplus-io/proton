drop stream if exists t;
create stream t (n int) engine MergeTree order by n;
insert into t values (1);
select 1 from t group by 1;
drop stream t;
