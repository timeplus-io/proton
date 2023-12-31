-- Tags: replica, no-replicated-database, no-parallel
-- Tag no-replicated-database: Fails due to additional replicas or shards

-- The test is mostly outdated as now every replica is leader and can do OPTIMIZE locally.
SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS rename1;
DROP STREAM IF EXISTS rename2;
DROP STREAM IF EXISTS rename3;
create stream rename1 (p int64, i int64, v uint64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/test/tables/rename', '1', v) PARTITION BY p ORDER BY i;
create stream rename2 (p int64, i int64, v uint64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/test/tables/rename', '2', v) PARTITION BY p ORDER BY i;

INSERT INTO rename1(p,i,v) VALUES (0, 1, 0);
INSERT INTO rename1(p,i,v) VALUES (0, 1, 1);

OPTIMIZE TABLE rename1 FINAL;
OPTIMIZE TABLE rename2 FINAL;
SELECT * FROM rename1;

RENAME STREAM rename2 TO rename3;

INSERT INTO rename1(p,i,v) VALUES (0, 1, 2);
SYSTEM SYNC REPLICA rename3; -- Make "rename3" to see all data parts.
OPTIMIZE TABLE rename3;
SYSTEM SYNC REPLICA rename1; -- Make "rename1" to see and process all scheduled merges.
SELECT * FROM rename1;

DROP STREAM IF EXISTS rename1;
DROP STREAM IF EXISTS rename2;
DROP STREAM IF EXISTS rename3;
