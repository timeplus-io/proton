-- Tags: distributed

-- set insert_distributed_sync = 1;  -- see https://github.com/ClickHouse/ClickHouse/issues/18971

DROP STREAM IF EXISTS local_01099_a;
DROP STREAM IF EXISTS local_01099_b;
DROP STREAM IF EXISTS distributed_01099_a;
DROP STREAM IF EXISTS distributed_01099_b;

SET parallel_distributed_insert_select=1;
SELECT 'parallel_distributed_insert_select=1';

--
-- test_shard_localhost
--

SELECT 'test_shard_localhost';

create stream local_01099_a (number uint64)  ;
create stream local_01099_b (number uint64)  ;
create stream distributed_01099_a AS local_01099_a ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_01099_a, rand());
create stream distributed_01099_b AS local_01099_b ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_01099_b, rand());

INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;

SELECT * FROM distributed_01099_b;

DROP STREAM local_01099_a;
DROP STREAM local_01099_b;
DROP STREAM distributed_01099_a;
DROP STREAM distributed_01099_b;

--
-- test_cluster_two_shards_localhost
--

SELECT 'test_cluster_two_shards_localhost';

create stream local_01099_a (number uint64)  ;
create stream local_01099_b (number uint64)  ;
create stream distributed_01099_a AS local_01099_a ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_01099_a, rand());
create stream distributed_01099_b AS local_01099_b ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_01099_b, rand());

INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;

SELECT number, count(number) FROM local_01099_b group by number order by number;

DROP STREAM local_01099_a;
DROP STREAM local_01099_b;
DROP STREAM distributed_01099_a;
DROP STREAM distributed_01099_b;

--
-- test_cluster_two_shards
--

SELECT 'test_cluster_two_shards';

create stream local_01099_a (number uint64)  ;
create stream local_01099_b (number uint64)  ;
create stream distributed_01099_a AS local_01099_a ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), local_01099_a, rand());
create stream distributed_01099_b AS local_01099_b ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), local_01099_b, rand());

SYSTEM STOP DISTRIBUTED SENDS distributed_01099_b;
SET prefer_localhost_replica=0; -- to require distributed send for local replica too
INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;
SET prefer_localhost_replica=1;

-- distributed sends disabled, 0 rows (since parallel_distributed_insert_select=1)
SELECT 'distributed';
SELECT number, count(number) FROM distributed_01099_b group by number order by number;
SYSTEM FLUSH DISTRIBUTED distributed_01099_b;

SELECT 'local';
SELECT number, count(number) FROM local_01099_b group by number order by number;
SELECT 'distributed';
SELECT number, count(number) FROM distributed_01099_b group by number order by number;

DROP STREAM local_01099_a;
DROP STREAM local_01099_b;
DROP STREAM distributed_01099_a;
DROP STREAM distributed_01099_b;


SET parallel_distributed_insert_select=2;
SELECT 'parallel_distributed_insert_select=2';

--
-- test_shard_localhost
--

SELECT 'test_shard_localhost';

create stream local_01099_a (number uint64)  ;
create stream local_01099_b (number uint64)  ;
create stream distributed_01099_a AS local_01099_a ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_01099_a, rand());
create stream distributed_01099_b AS local_01099_b ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_01099_b, rand());

INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;

SELECT * FROM distributed_01099_b;

DROP STREAM local_01099_a;
DROP STREAM local_01099_b;
DROP STREAM distributed_01099_a;
DROP STREAM distributed_01099_b;

--
-- test_cluster_two_shards_localhost
--

SELECT 'test_cluster_two_shards_localhost';

-- Log engine will lead to deadlock:
--     DB::Exception: std::system_error: Resource deadlock avoided.
-- So use MergeTree instead.
create stream local_01099_a (number uint64) ENGINE = MergeTree() ORDER BY number;
create stream local_01099_b (number uint64) ENGINE = MergeTree() ORDER BY number;
create stream distributed_01099_a AS local_01099_a ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_01099_a, rand());
create stream distributed_01099_b AS local_01099_b ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_01099_b, rand());

INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;

SELECT number, count(number) FROM local_01099_b group by number order by number;

DROP STREAM local_01099_a;
DROP STREAM local_01099_b;
DROP STREAM distributed_01099_a;
DROP STREAM distributed_01099_b;

--
-- test_cluster_two_shards
--

SELECT 'test_cluster_two_shards';

create stream local_01099_a (number uint64) ENGINE = MergeTree() ORDER BY number;
create stream local_01099_b (number uint64) ENGINE = MergeTree() ORDER BY number;
create stream distributed_01099_a AS local_01099_a ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), local_01099_a, rand());
create stream distributed_01099_b AS local_01099_b ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), local_01099_b, rand());

SYSTEM STOP DISTRIBUTED SENDS distributed_01099_b;
SET prefer_localhost_replica=0; -- to require distributed send for local replica too
INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;
SET prefer_localhost_replica=1;

-- distributed sends disabled, but they are not required, since insert is done into local table.
-- (since parallel_distributed_insert_select=2)
SELECT 'distributed';
SELECT number, count(number) FROM distributed_01099_b group by number order by number;
SELECT 'local';
SELECT number, count(number) FROM local_01099_b group by number order by number;

DROP STREAM local_01099_a;
DROP STREAM local_01099_b;
DROP STREAM distributed_01099_a;
DROP STREAM distributed_01099_b;
