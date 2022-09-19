DROP STREAM IF EXISTS t_02147;
DROP STREAM IF EXISTS t_02147_dist;
DROP STREAM IF EXISTS t_02147_merge;

create stream t_02147 (date DateTime, v uint32)
ENGINE = MergeTree ORDER BY to_start_of_hour(date);

create stream t_02147_dist AS t_02147 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_02147);
create stream t_02147_merge AS t_02147 ENGINE = Merge(currentDatabase(), 't_02147');

SET optimize_monotonous_functions_in_order_by = 1;

EXPLAIN SYNTAX SELECT * FROM t_02147 ORDER BY to_start_of_hour(date), v;
EXPLAIN SYNTAX SELECT * FROM t_02147_dist ORDER BY to_start_of_hour(date), v;
EXPLAIN SYNTAX SELECT * FROM t_02147_merge ORDER BY to_start_of_hour(date), v;
