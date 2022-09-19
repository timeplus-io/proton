-- Tags: long, replica

DROP STREAM IF EXISTS minmax_idx;
DROP STREAM IF EXISTS minmax_idx_r;
DROP STREAM IF EXISTS minmax_idx2;
DROP STREAM IF EXISTS minmax_idx2_r;

SET replication_alter_partitions_sync = 2;

create stream minmax_idx
(
    u64 uint64,
    i32 int32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00836/indices_alter1', 'r1')
ORDER BY u64;

create stream minmax_idx_r
(
    u64 uint64,
    i32 int32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00836/indices_alter1', 'r2')
ORDER BY u64;

INSERT INTO minmax_idx VALUES (1, 2);

SYSTEM SYNC REPLICA minmax_idx_r;

ALTER STREAM minmax_idx ADD INDEX idx1 u64 * i32 TYPE minmax GRANULARITY 10;
ALTER STREAM minmax_idx_r ADD INDEX idx2 u64 + i32 TYPE minmax GRANULARITY 10;
ALTER STREAM minmax_idx ADD INDEX idx3 u64 - i32 TYPE minmax GRANULARITY 10 AFTER idx1;

SHOW create stream minmax_idx;
SHOW create stream minmax_idx_r;

SELECT * FROM minmax_idx WHERE u64 * i32 = 2 ORDER BY (u64, i32);
SELECT * FROM minmax_idx_r WHERE u64 * i32 = 2 ORDER BY (u64, i32);

INSERT INTO minmax_idx VALUES (1, 4);
INSERT INTO minmax_idx_r VALUES (3, 2);
INSERT INTO minmax_idx VALUES (1, 5);
INSERT INTO minmax_idx_r VALUES (65, 75);
INSERT INTO minmax_idx VALUES (19, 9);

SYSTEM SYNC REPLICA minmax_idx;
SYSTEM SYNC REPLICA minmax_idx_r;

SELECT * FROM minmax_idx WHERE u64 * i32 > 1 ORDER BY (u64, i32);
SELECT * FROM minmax_idx_r WHERE u64 * i32 > 1 ORDER BY (u64, i32);

ALTER STREAM minmax_idx DROP INDEX idx1;

SHOW create stream minmax_idx;
SHOW create stream minmax_idx_r;

SELECT * FROM minmax_idx WHERE u64 * i32 > 1 ORDER BY (u64, i32);
SELECT * FROM minmax_idx_r WHERE u64 * i32 > 1 ORDER BY (u64, i32);

ALTER STREAM minmax_idx DROP INDEX idx2;
ALTER STREAM minmax_idx_r DROP INDEX idx3;

SHOW create stream minmax_idx;
SHOW create stream minmax_idx_r;

ALTER STREAM minmax_idx ADD INDEX idx1 u64 * i32 TYPE minmax GRANULARITY 10;

SHOW create stream minmax_idx;
SHOW create stream minmax_idx_r;

SELECT * FROM minmax_idx WHERE u64 * i32 > 1 ORDER BY (u64, i32);
SELECT * FROM minmax_idx_r WHERE u64 * i32 > 1 ORDER BY (u64, i32);


create stream minmax_idx2
(
    u64 uint64,
    i32 int32,
    INDEX idx1 u64 + i32 TYPE minmax GRANULARITY 10,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 10
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00836/indices_alter2', 'r1')
ORDER BY u64;

create stream minmax_idx2_r
(
    u64 uint64,
    i32 int32,
    INDEX idx1 u64 + i32 TYPE minmax GRANULARITY 10,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 10
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00836/indices_alter2', 'r2')
ORDER BY u64;


SHOW create stream minmax_idx2;
SHOW create stream minmax_idx2_r;

INSERT INTO minmax_idx2 VALUES (1, 2);
INSERT INTO minmax_idx2_r VALUES (1, 3);

SYSTEM SYNC REPLICA minmax_idx2;
SYSTEM SYNC REPLICA minmax_idx2_r;

SELECT * FROM minmax_idx2 WHERE u64 * i32 >= 2 ORDER BY (u64, i32);
SELECT * FROM minmax_idx2_r WHERE u64 * i32 >= 2 ORDER BY (u64, i32);

ALTER STREAM minmax_idx2_r DROP INDEX idx1, DROP INDEX idx2;

SHOW create stream minmax_idx2;
SHOW create stream minmax_idx2_r;

SELECT * FROM minmax_idx2 WHERE u64 * i32 >= 2 ORDER BY (u64, i32);
SELECT * FROM minmax_idx2_r WHERE u64 * i32 >= 2 ORDER BY (u64, i32);

DROP STREAM minmax_idx;
DROP STREAM minmax_idx_r;
DROP STREAM minmax_idx2;
DROP STREAM minmax_idx2_r;
