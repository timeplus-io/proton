-- Tags: long, replica, no-replicated-database
-- Tag no-replicated-database: Fails due to additional replicas or shards

SET replication_alter_partitions_sync = 2;

DROP STREAM IF EXISTS replica1;
DROP STREAM IF EXISTS replica2;

create stream replica1 (v uint8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/'||currentDatabase()||'test/01451/attach', 'r1') order by tuple() settings max_replicated_merges_in_queue = 0;
create stream replica2 (v uint8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/'||currentDatabase()||'test/01451/attach', 'r2') order by tuple() settings max_replicated_merges_in_queue = 0;

INSERT INTO replica1 VALUES (0);
INSERT INTO replica1 VALUES (1);
INSERT INTO replica1 VALUES (2);

ALTER STREAM replica1 DETACH PART 'all_100_100_0'; -- { serverError 232 }

SELECT v FROM replica1 ORDER BY v;

SYSTEM SYNC REPLICA replica2;
ALTER STREAM replica2 DETACH PART 'all_1_1_0';

SELECT v FROM replica1 ORDER BY v;

SELECT name FROM system.detached_parts WHERE table = 'replica2' AND database = currentDatabase();

ALTER STREAM replica2 ATTACH PART 'all_1_1_0';

SYSTEM SYNC REPLICA replica1;
SELECT v FROM replica1 ORDER BY v;

SELECT name FROM system.detached_parts WHERE table = 'replica2' AND database = currentDatabase();

SELECT '-- drop part --';

ALTER STREAM replica1 DROP PART 'all_3_3_0';

ALTER STREAM replica1 ATTACH PART 'all_3_3_0'; -- { serverError 233 }

SELECT v FROM replica1 ORDER BY v;

SELECT '-- resume merges --';

ALTER STREAM replica1 MODIFY SETTING max_replicated_merges_in_queue = 1;
OPTIMIZE STREAM replica1 FINAL;

SELECT v FROM replica1 ORDER BY v;

SELECT name FROM system.parts WHERE table = 'replica2' AND active AND database = currentDatabase();

DROP STREAM replica1;
DROP STREAM replica2;
