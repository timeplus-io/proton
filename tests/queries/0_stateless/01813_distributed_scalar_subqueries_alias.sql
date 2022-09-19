-- Tags: distributed

DROP STREAM IF EXISTS data;
create stream data (a int64, b int64) ();

DROP STREAM IF EXISTS data_distributed;
create stream data_distributed (a int64, b int64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'data');

INSERT INTO data VALUES (0, 0);

SET prefer_localhost_replica = 1;
SELECT a / (SELECT sum(number) FROM numbers(10)) FROM data_distributed;
SELECT a < (SELECT 1) FROM data_distributed;

SET prefer_localhost_replica = 0;
SELECT a / (SELECT sum(number) FROM numbers(10)) FROM data_distributed;
SELECT a < (SELECT 1) FROM data_distributed;

DROP STREAM data_distributed;
DROP STREAM data;
