-- Tags: distributed

DROP STREAM IF EXISTS local;
DROP STREAM IF EXISTS distributed;

create stream local (x uint8) ;
create stream distributed AS local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local, x);

SET insert_distributed_sync = 0, network_compression_method = 'zstd';

INSERT INTO distributed SELECT number FROM numbers(256);
SYSTEM FLUSH DISTRIBUTED distributed;

SELECT count() FROM local;
SELECT count() FROM distributed;

DROP STREAM local;
DROP STREAM distributed;
