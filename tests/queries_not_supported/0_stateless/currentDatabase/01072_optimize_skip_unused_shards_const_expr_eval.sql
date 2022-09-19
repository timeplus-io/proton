-- Tags: shard
SET query_mode = 'table';
drop stream if exists data_01072;
drop stream if exists dist_01072;

set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=1;

create stream data_01072 (key int, value int, str string) Engine=Null();
create stream dist_01072 (key int, value int, str string) Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01072, key%2);

select * from dist_01072 where key=0 and length(str)=0;
select * from dist_01072 where key=0 and str='';
select * from dist_01072 where xxHash64(0)==xxHash64(0) and key=0;
select * from dist_01072 where key=toInt32OrZero(to_string(xxHash64(0)));
select * from dist_01072 where key=to_int32(xxHash32(0));
select * from dist_01072 where key=to_int32(to_int32(xxHash32(0)));
select * from dist_01072 where key=to_int32(to_int32(to_int32(xxHash32(0))));
select * from dist_01072 where key=value; -- { serverError 507; }
select * from dist_01072 where key=to_int32(value); -- { serverError 507; }
select * from dist_01072 where key=value settings force_optimize_skip_unused_shards=0;
select * from dist_01072 where key=to_int32(value) settings force_optimize_skip_unused_shards=0;

drop stream dist_01072;
create stream dist_01072 (key int, value Nullable(int), str string) Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01072, key%2);
select * from dist_01072 where key=to_int32(xxHash32(0));
select * from dist_01072 where key=value; -- { serverError 507; }
select * from dist_01072 where key=to_int32(value); -- { serverError 507; }
select * from dist_01072 where key=value settings force_optimize_skip_unused_shards=0;
select * from dist_01072 where key=to_int32(value) settings force_optimize_skip_unused_shards=0;

set allow_suspicious_low_cardinality_types=1;

drop stream dist_01072;
create stream dist_01072 (key int, value LowCardinality(int), str string) Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01072, key%2);
select * from dist_01072 where key=to_int32(xxHash32(0));
select * from dist_01072 where key=value; -- { serverError 507; }
select * from dist_01072 where key=to_int32(value); -- { serverError 507; }
select * from dist_01072 where key=value settings force_optimize_skip_unused_shards=0;
select * from dist_01072 where key=to_int32(value) settings force_optimize_skip_unused_shards=0;

drop stream dist_01072;
create stream dist_01072 (key int, value LowCardinality(Nullable(int)), str string) Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01072, key%2);
select * from dist_01072 where key=to_int32(xxHash32(0));
select * from dist_01072 where key=value; -- { serverError 507; }
select * from dist_01072 where key=to_int32(value); -- { serverError 507; }
select * from dist_01072 where key=value settings force_optimize_skip_unused_shards=0;
select * from dist_01072 where key=to_int32(value) settings force_optimize_skip_unused_shards=0;

-- check virtual columns
drop stream data_01072;
drop stream dist_01072;
create stream data_01072 (key int) Engine=MergeTree() ORDER BY key;
create stream dist_01072 (key int) Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01072, key);
select * from dist_01072 where key=0 and _part='0' settings force_optimize_skip_unused_shards=2;

drop stream data_01072;
drop stream dist_01072;
