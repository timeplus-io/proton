-- Tags: long, zookeeper

 

DROP STREAM IF EXISTS quorum1 SYNC;
DROP STREAM IF EXISTS quorum2 SYNC;
DROP STREAM IF EXISTS quorum3 SYNC;

create stream quorum1(x uint32, y date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_01513/sequence_consistency', '1') ORDER BY x PARTITION BY y;
create stream quorum2(x uint32, y date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_01513/sequence_consistency', '2') ORDER BY x PARTITION BY y;
create stream quorum3(x uint32, y date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_01513/sequence_consistency', '3') ORDER BY x PARTITION BY y;

INSERT INTO quorum1 VALUES (1, '1990-11-15');
INSERT INTO quorum1 VALUES (2, '1990-11-15');
INSERT INTO quorum1 VALUES (3, '2020-12-16');

SYSTEM SYNC REPLICA quorum2;
SYSTEM SYNC REPLICA quorum3;

SET select_sequential_consistency=0;
SET optimize_trivial_count_query=1;
SET insert_quorum=2, insert_quorum_parallel=0;

SYSTEM STOP FETCHES quorum1;

INSERT INTO quorum2 VALUES (4, to_date('2020-12-16'));

SYSTEM SYNC REPLICA quorum3;

-- Should read local committed parts instead of throwing error code: 289. DB::Exception: Replica doesn't have part 20201216_1_1_0 which was successfully written to quorum of other replicas.
SELECT count() FROM quorum1;

SELECT count() FROM quorum2;
SELECT count() FROM quorum3;

DROP STREAM quorum1 SYNC;
DROP STREAM quorum2 SYNC;
DROP STREAM quorum3 SYNC;
