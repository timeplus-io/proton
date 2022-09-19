-- Tags: shard

DROP STREAM IF EXISTS t_local;
DROP STREAM IF EXISTS t_distr;

create stream t_local (a int) ;
create stream t_distr (a int) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 't_local');

INSERT INTO t_local VALUES (1), (2);
SET optimize_skip_unused_shards = 1;
SELECT * FROM t_distr WHERE a = 1;

DROP table t_local;
DROP table t_distr;
