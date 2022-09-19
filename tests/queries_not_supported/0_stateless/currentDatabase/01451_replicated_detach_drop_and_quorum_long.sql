-- Tags: long, replica, no-replicated-database

SET replication_alter_partitions_sync = 2;


DROP STREAM IF EXISTS replica1;
DROP STREAM IF EXISTS replica2;

create stream replica1 (v uint8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/01451/quorum', 'r1') order by tuple() settings max_replicated_merges_in_queue = 0;
create stream replica2 (v uint8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/01451/quorum', 'r2') order by tuple() settings max_replicated_merges_in_queue = 0;

INSERT INTO replica1 VALUES (0);

SYSTEM SYNC REPLICA replica2;

SELECT name FROM system.parts WHERE table = 'replica2' and database = currentDatabase() and active = 1;

ALTER STREAM replica2 DETACH PART 'all_0_0_0';

SELECT * FROM replica1;

SELECT * FROM replica2;

-- drop of empty partition works
ALTER STREAM replica2 DROP PARTITION ID 'all';

SET insert_quorum = 2, insert_quorum_parallel = 0;

INSERT INTO replica2 VALUES (1);

SYSTEM SYNC REPLICA replica2;

ALTER STREAM replica1 DETACH PART 'all_2_2_0'; --{serverError 48}

SELECT name FROM system.parts WHERE table = 'replica1' and database = currentDatabase() and active = 1 ORDER BY name;

SELECT COUNT() FROM replica1;

SET insert_quorum_parallel=1;

INSERT INTO replica2 VALUES (2);

-- should work, parallel quorum nodes exists only during insert
ALTER STREAM replica1 DROP PART 'all_3_3_0';

SELECT name FROM system.parts WHERE table = 'replica1' and database = currentDatabase() and active = 1 ORDER BY name;

SELECT COUNT() FROM replica1;

DROP STREAM IF EXISTS replica1;
DROP STREAM IF EXISTS replica2;
