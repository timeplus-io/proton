DROP STREAM IF EXISTS mutation_1;
DROP STREAM IF EXISTS mutation_2;

CREATE STREAM mutation_1
(
    a uint64,
    b string
)
ENGINE = ReplicatedMergeTree('/clickhouse/test/{database}/t', '1')
ORDER BY tuple() SETTINGS min_bytes_for_wide_part=0, allow_remote_fs_zero_copy_replication=1;

CREATE STREAM mutation_2
(
    a uint64,
    b string
)
ENGINE = ReplicatedMergeTree('/clickhouse/test/{database}/t', '2')
ORDER BY tuple() SETTINGS min_bytes_for_wide_part=0, allow_remote_fs_zero_copy_replication=1;

INSERT INTO mutation_1 VALUES (1, 'Hello');

SYSTEM SYNC REPLICA mutation_2;

SYSTEM STOP REPLICATION QUEUES mutation_2;

ALTER STREAM mutation_1 UPDATE a = 2 WHERE b = 'xxxxxx' SETTINGS mutations_sync=1;

SELECT * from mutation_1;
SELECT * from mutation_2;

DROP STREAM mutation_1 SYNC;

SELECT * from mutation_2;

DROP STREAM IF EXISTS mutation_1;
DROP STREAM IF EXISTS mutation_2;
