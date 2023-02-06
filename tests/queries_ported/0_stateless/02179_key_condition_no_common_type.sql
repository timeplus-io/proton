drop stream if exists t;

create stream t (c decimal32(9)) engine MergeTree order by c;

insert into t values (0.9);

select * from t where c < 1.2;

drop stream t;
