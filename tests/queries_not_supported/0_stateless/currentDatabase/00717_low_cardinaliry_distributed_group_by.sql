-- Tags: distributed

set insert_distributed_sync = 1;
set allow_suspicious_low_cardinality_types = 1;

DROP STREAM IF EXISTS test_low_null_float;
DROP STREAM IF EXISTS dist_00717;

create stream test_low_null_float (a LowCardinality(Nullable(float64))) ;
create stream dist_00717 (a LowCardinality(Nullable(float64))) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), 'test_low_null_float', rand());

INSERT INTO dist_00717 (a) SELECT number FROM system.numbers LIMIT 1000000;
SELECT a, count() FROM dist_00717 GROUP BY a ORDER BY a ASC, count() ASC LIMIT 10;

DROP STREAM IF EXISTS test_low_null_float;
DROP STREAM IF EXISTS dist_00717;
