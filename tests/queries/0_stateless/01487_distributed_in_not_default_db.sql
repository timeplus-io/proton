-- Tags: distributed, no-parallel

CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;
CREATE DATABASE IF NOT EXISTS main_01487;
CREATE DATABASE IF NOT EXISTS test_01487;

USE main_01487;

DROP STREAM IF EXISTS shard_0.l;
DROP STREAM IF EXISTS shard_1.l;
DROP STREAM IF EXISTS d;
DROP STREAM IF EXISTS t;

create stream shard_0.l (value uint8) ENGINE = MergeTree ORDER BY value;
create stream shard_1.l (value uint8) ENGINE = MergeTree ORDER BY value;
create stream t (value uint8) ;

INSERT INTO shard_0.l VALUES (0);
INSERT INTO shard_1.l VALUES (1);
INSERT INTO t VALUES (0), (1), (2);

create stream d AS t ENGINE = Distributed(test_cluster_two_shards_different_databases, currentDatabase(), t);

USE test_01487;
DROP DATABASE test_01487;

SELECT * FROM main_01487.d WHERE value IN (SELECT l.value FROM l) ORDER BY value;

USE main_01487;

DROP STREAM IF EXISTS shard_0.l;
DROP STREAM IF EXISTS shard_1.l;
DROP STREAM IF EXISTS d;
DROP STREAM IF EXISTS t;

DROP DATABASE shard_0;
DROP DATABASE shard_1;
DROP DATABASE main_01487;
