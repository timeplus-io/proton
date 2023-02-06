drop stream if exists t1;

set allow_suspicious_low_cardinality_types = 1;
create stream t1 (id low_cardinality(nullable(int64))) engine MergeTree order by id settings allow_nullable_key = 1, index_granularity = 1;

insert into t1 values (21585718595728998), (null);

select * from t1 where id = 21585718595728998;

drop stream t1;
