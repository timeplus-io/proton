-- Tags: distributed

set insert_distributed_sync=1;

DROP STREAM IF EXISTS dist_00967;
DROP STREAM IF EXISTS underlying_00967;

-- To suppress "Structure does not match (...), implicit conversion will be done." message
SET send_logs_level='error';

create stream dist_00967 (key uint64) Engine=Distributed('test_shard_localhost', currentDatabase(), underlying_00967);
create stream underlying_00967 (key Nullable(uint64)) Engine=TinyLog();
INSERT INTO dist_00967 SELECT to_uint64(number) FROM system.numbers LIMIT 1;

SELECT * FROM dist_00967;

DROP STREAM dist_00967;
DROP STREAM underlying_00967;
