drop stream if exists test;

create stream test(a int64) Engine=MergeTree order by tuple();

set optimize_trivial_count_query=1, empty_result_for_aggregation_by_empty_set=1;

select count() from test;

drop stream test;
