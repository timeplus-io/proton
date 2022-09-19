-- Tags: shard

DROP STREAM IF EXISTS x;
DROP STREAM IF EXISTS x_dist;
DROP STREAM IF EXISTS y;
DROP STREAM IF EXISTS y_dist;

create stream x AS system.numbers ENGINE = MergeTree ORDER BY number;
create stream y AS system.numbers ENGINE = MergeTree ORDER BY number;

create stream x_dist as x ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), x);
create stream y_dist as y ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), y);

-- insert into first shard
INSERT INTO x_dist SELECT * FROM numbers(10) settings insert_shard_id = 1;
INSERT INTO y_dist SELECT * FROM numbers(10) settings insert_shard_id = 1;

SELECT * FROM x_dist ORDER by number;
SELECT * FROM y_dist ORDER by number;

-- insert into second shard
INSERT INTO x_dist SELECT * FROM numbers(10, 10) settings insert_shard_id = 2;
INSERT INTO y_dist SELECT * FROM numbers(10, 10) settings insert_shard_id = 2;

SELECT * FROM x_dist ORDER by number;
SELECT * FROM y_dist ORDER by number;

-- no sharding key
INSERT INTO x_dist SELECT * FROM numbers(10); -- { serverError 55 }
INSERT INTO y_dist SELECT * FROM numbers(10); -- { serverError 55 }

-- invalid shard id
INSERT INTO x_dist SELECT * FROM numbers(10) settings insert_shard_id = 3; -- { serverError 577 }
INSERT INTO y_dist SELECT * FROM numbers(10) settings insert_shard_id = 3; -- { serverError 577 }

DROP STREAM x;
DROP STREAM x_dist;
DROP STREAM y;
DROP STREAM y_dist;
