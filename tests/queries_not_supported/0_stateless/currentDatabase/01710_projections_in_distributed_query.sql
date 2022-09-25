-- Tags: distributed
SET query_mode = 'table';
drop stream if exists projection_test;

create stream projection_test (dt datetime, cost int64, projection p (select to_start_of_minute(dt) dt_m, sum(cost) group by dt_m)) engine MergeTree partition by to_date(dt) order by dt;

insert into projection_test with rowNumberInAllBlocks() as id select to_datetime('2020-10-24 00:00:00') + (id / 20), * from generateRandom('cost int64', 10, 10, 1) limit 1000 settings max_threads = 1;

set allow_experimental_projection_optimization = 1, force_optimize_projection = 1;

select to_start_of_minute(dt) dt_m, sum(cost) from projection_test group by dt_m;
select sum(cost) from projection_test;

drop stream if exists projection_test_d;

create stream projection_test_d (dt datetime, cost int64) engine Distributed(test_cluster_two_shards, currentDatabase(), projection_test);

select to_start_of_minute(dt) dt_m, sum(cost) from projection_test_d group by dt_m;
select sum(cost) from projection_test_d;

drop stream projection_test;
drop stream projection_test_d;
