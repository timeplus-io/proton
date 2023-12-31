DROP STREAM IF EXISTS x;
DROP STREAM IF EXISTS y;

create stream x AS system.numbers ENGINE = MergeTree ORDER BY number;
create stream y AS system.numbers ENGINE = MergeTree ORDER BY number;

-- Just one shard, sharding key isn't necessary
INSERT INTO FUNCTION cluster('test_shard_localhost', currentDatabase(), x) SELECT * FROM numbers(10);
INSERT INTO FUNCTION cluster('test_shard_localhost', currentDatabase(), x, rand()) SELECT * FROM numbers(10);

-- More than one shard, sharding key is necessary
INSERT INTO FUNCTION cluster('test_cluster_two_shards_localhost', currentDatabase(), x) SELECT * FROM numbers(10); --{ serverError 55 }
INSERT INTO FUNCTION cluster('test_cluster_two_shards_localhost', currentDatabase(), x, rand()) SELECT * FROM numbers(10);

INSERT INTO FUNCTION remote('127.0.0.{1,2}', currentDatabase(), y, 'default') SELECT * FROM numbers(10); -- { serverError 55 }
INSERT INTO FUNCTION remote('127.0.0.{1,2}', currentDatabase(), y, 'default', rand()) SELECT * FROM numbers(10);

SELECT * FROM x ORDER BY number;

SELECT * FROM remote('127.0.0.{1,2}', currentDatabase(), y) ORDER BY number;

DROP STREAM x;
DROP STREAM y;
