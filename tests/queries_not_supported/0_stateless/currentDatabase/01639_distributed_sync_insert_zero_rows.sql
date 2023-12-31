-- Tags: distributed

DROP STREAM IF EXISTS local;
DROP STREAM IF EXISTS distributed;

create stream local (x uint8) ;
create stream distributed AS local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local, x);

SET insert_distributed_sync = 1;

INSERT INTO distributed SELECT number FROM numbers(256) WHERE number % 2 = 0;
SELECT count() FROM local;
SELECT count() FROM distributed;

TRUNCATE TABLE local;
INSERT INTO distributed SELECT number FROM numbers(256) WHERE number % 2 = 1;
SELECT count() FROM local;
SELECT count() FROM distributed;

TRUNCATE TABLE local;
INSERT INTO distributed SELECT number FROM numbers(256) WHERE number < 128;
SELECT count() FROM local;
SELECT count() FROM distributed;

DROP STREAM local;
DROP STREAM distributed;
