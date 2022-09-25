-- Tags: deadlock, distributed

DROP STREAM IF EXISTS t_local;
DROP STREAM IF EXISTS t_dist;

create stream t_local(a int) engine Log;
create stream t_dist (a int) engine Distributed(test_shard_localhost, currentDatabase(), 't_local', cityHash64(a));

set insert_distributed_sync = 1;

insert into t_dist values (1);

DROP STREAM t_local;
DROP STREAM t_dist;
