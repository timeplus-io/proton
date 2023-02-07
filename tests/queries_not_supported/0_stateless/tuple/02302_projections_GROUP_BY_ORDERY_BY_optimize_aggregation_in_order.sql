drop stream if exists test_agg_proj_02302;

create stream test_agg_proj_02302 (x int32, y int32, PROJECTION x_plus_y (select sum(x - y), argMax(x, y) group by x + y)) ENGINE = MergeTree order by tuple() settings index_granularity = 1;
insert into test_agg_proj_02302 select int_div(number, 2), -int_div(number,3) - 1 from numbers(100);

-- { echoOn }
select x + y, sum(x - y) as s from test_agg_proj_02302 group by x + y order by s desc limit 5 settings optimize_aggregation_in_order=0, optimize_read_in_order=0;
select x + y, sum(x - y) as s from test_agg_proj_02302 group by x + y order by s desc limit 5 settings optimize_aggregation_in_order=1, optimize_read_in_order=1;

-- { echoOff }
drop stream test_agg_proj_02302;
