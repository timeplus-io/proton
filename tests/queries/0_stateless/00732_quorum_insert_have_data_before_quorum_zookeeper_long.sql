-- Tags: long, zookeeper, no-replicated-database
-- Tag no-replicated-database: Fails due to additional replicas or shards

SET send_logs_level = 'fatal';

DROP STREAM IF EXISTS quorum1;
DROP STREAM IF EXISTS quorum2;

create stream quorum1(x uint32, y date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00732/quorum_have_data', '1') ORDER BY x PARTITION BY y;
create stream quorum2(x uint32, y date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00732/quorum_have_data', '2') ORDER BY x PARTITION BY y;

INSERT INTO quorum1 VALUES (1, '1990-11-15');
INSERT INTO quorum1 VALUES (2, '1990-11-15');
INSERT INTO quorum1 VALUES (3, '2020-12-16');

SYSTEM SYNC REPLICA quorum2;

SET select_sequential_consistency=1;

SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;

SET insert_quorum=2, insert_quorum_parallel=0;

INSERT INTO quorum1 VALUES (4, '1990-11-15');
INSERT INTO quorum1 VALUES (5, '1990-11-15');
INSERT INTO quorum1 VALUES (6, '2020-12-16');

SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;

DROP STREAM quorum1;
DROP STREAM quorum2;
