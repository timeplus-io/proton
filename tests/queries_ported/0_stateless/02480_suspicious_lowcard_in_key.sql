set allow_suspicious_low_cardinality_types=1;

drop stream if exists test;

create stream test (val low_cardinality(float32)) engine MergeTree order by val;

insert into test values (nan);

select count() from test where to_uint64(val) = -1; -- { serverError 70 }

drop stream if exists test;
