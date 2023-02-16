DROP STREAM IF EXISTS mutate_and_zero_copy_replication1;
DROP STREAM IF EXISTS mutate_and_zero_copy_replication2;

CREATE STREAM mutate_and_zero_copy_replication1
(
    a uint64,
    b string,
    c float64
)
ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02427_mutate_and_zero_copy_replication/alter', '1')
ORDER BY tuple()
SETTINGS old_parts_lifetime=0, cleanup_delay_period=300, cleanup_delay_period_random_add=300, min_bytes_for_wide_part = 0;

CREATE STREAM mutate_and_zero_copy_replication2
(
    a uint64,
    b string,
    c float64
)
ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02427_mutate_and_zero_copy_replication/alter', '2')
ORDER BY tuple()
SETTINGS old_parts_lifetime=0, cleanup_delay_period=300, cleanup_delay_period_random_add=300;


INSERT INTO mutate_and_zero_copy_replication1 VALUES (1, '1', 1.0);
SYSTEM SYNC REPLICA mutate_and_zero_copy_replication2;

SET mutations_sync=2;

ALTER STREAM mutate_and_zero_copy_replication1 UPDATE a = 2 WHERE 1;

DROP STREAM mutate_and_zero_copy_replication1 SYNC;

DETACH STREAM mutate_and_zero_copy_replication2;
ATTACH STREAM mutate_and_zero_copy_replication2;

SELECT * FROM mutate_and_zero_copy_replication2 WHERE NOT ignore(*);

DROP STREAM IF EXISTS mutate_and_zero_copy_replication1;
DROP STREAM IF EXISTS mutate_and_zero_copy_replication2;
