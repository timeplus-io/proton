-- Tags: distributed

DROP STREAM IF EXISTS data;
DROP STREAM IF EXISTS dist;

create stream data (key string) Engine=Memory();
create stream dist (key low_cardinality(string)) engine=Distributed(test_cluster_two_shards, currentDatabase(), data);
insert into data values ('foo');
set distributed_aggregation_memory_efficient=1;
select * from dist group by key;

DROP STREAM data;
DROP STREAM dist;
