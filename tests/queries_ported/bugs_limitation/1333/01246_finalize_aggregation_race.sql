-- Tags: race
SET query_mode = 'table';
drop stream if exists test_quantile;
create stream test_quantile (x aggregate_function(quantile_timing(0.2), uint64)) engine = Memory;
insert into test_quantile select medianTimingState(.2)(number) from (select * from numbers(1000) order by number desc);
select y from (
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile union all
select finalize_aggregation(x) as y from test_quantile)
order by y;
drop stream test_quantile;
