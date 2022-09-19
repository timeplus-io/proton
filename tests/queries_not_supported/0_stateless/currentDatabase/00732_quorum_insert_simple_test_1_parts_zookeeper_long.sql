-- Tags: long, zookeeper, no-replicated-database
-- Tag no-replicated-database: Fails due to additional replicas or shards

SET send_logs_level = 'fatal';

DROP STREAM IF EXISTS quorum1;
DROP STREAM IF EXISTS quorum2;

create stream quorum1(x uint32, y date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00732/quorum1', '1') ORDER BY x PARTITION BY y;
create stream quorum2(x uint32, y date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00732/quorum1', '2') ORDER BY x PARTITION BY y;

SET insert_quorum=2, insert_quorum_parallel=0;
SET select_sequential_consistency=1;

INSERT INTO quorum1 VALUES (1, '2018-11-15');
INSERT INTO quorum1 VALUES (2, '2018-11-15');

SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;

OPTIMIZE STREAM quorum1 PARTITION '2018-11-15' FINAL;

-- everything works fine after merge
SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;

SELECT count(*) FROM system.parts WHERE active AND database = currentDatabase() AND table='quorum1';

INSERT INTO quorum1 VALUES (3, '2018-11-15');
INSERT INTO quorum1 VALUES (4, '2018-11-15');

-- and after we add new parts
SELECT sum(x) FROM quorum1;
SELECT sum(x) FROM quorum2;

DROP STREAM quorum1;
DROP STREAM quorum2;
