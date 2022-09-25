-- Tags: shard
SET query_mode = 'table';
set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=1;

drop stream if exists d;
drop stream if exists dp;

create stream d (i uint8) Engine=Memory;
create stream dp as d Engine=Distributed(test_cluster_two_shards, currentDatabase(), d, i);

insert into d values (1), (2);

select * from dp where i in (1);

drop stream if exists d;
drop stream if exists dp;
