-- Tags: zookeeper, no-replicated-database, no-parallel
-- Tag no-replicated-database: Unsupported type of ALTER query

DROP STREAM IF EXISTS replicated_table_for_alter1;
DROP STREAM IF EXISTS replicated_table_for_alter2;

SET replication_alter_partitions_sync = 2;

create stream replicated_table_for_alter1 (
  id uint64,
  Data string
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_00980_{database}/replicated_table_for_alter', '1') ORDER BY id;

create stream replicated_table_for_alter2 (
  id uint64,
  Data string
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_00980_{database}/replicated_table_for_alter', '2') ORDER BY id;

SHOW create stream replicated_table_for_alter1;

ALTER STREAM replicated_table_for_alter1 MODIFY SETTING index_granularity = 4096; -- { serverError 472 }

SHOW create stream replicated_table_for_alter1;

INSERT INTO replicated_table_for_alter2 VALUES (1, '1'), (2, '2');

SYSTEM SYNC REPLICA replicated_table_for_alter1;

ALTER STREAM replicated_table_for_alter1 MODIFY SETTING use_minimalistic_part_header_in_zookeeper = 1;

INSERT INTO replicated_table_for_alter1 VALUES (3, '3'), (4, '4');

SYSTEM SYNC REPLICA replicated_table_for_alter2;

SELECT COUNT() FROM replicated_table_for_alter1;
SELECT COUNT() FROM replicated_table_for_alter2;

DETACH TABLE replicated_table_for_alter2;
ATTACH TABLE replicated_table_for_alter2;

DETACH TABLE replicated_table_for_alter1;
ATTACH TABLE replicated_table_for_alter1;

SELECT COUNT() FROM replicated_table_for_alter1;
SELECT COUNT() FROM replicated_table_for_alter2;

ALTER STREAM replicated_table_for_alter2 MODIFY SETTING  parts_to_throw_insert = 1, parts_to_delay_insert = 1;
INSERT INTO replicated_table_for_alter2 VALUES (3, '1'), (4, '2'); -- { serverError 252 }

INSERT INTO replicated_table_for_alter1 VALUES (5, '5'), (6, '6');

SYSTEM SYNC REPLICA replicated_table_for_alter2;

SELECT COUNT() FROM replicated_table_for_alter1;
SELECT COUNT() FROM replicated_table_for_alter2;

DETACH TABLE replicated_table_for_alter2;
ATTACH TABLE replicated_table_for_alter2;

DETACH TABLE replicated_table_for_alter1;
ATTACH TABLE replicated_table_for_alter1;

SHOW create stream replicated_table_for_alter1;
SHOW create stream replicated_table_for_alter2;

ALTER STREAM replicated_table_for_alter1 ADD COLUMN Data2 uint64, MODIFY SETTING check_delay_period=5, check_delay_period=10, check_delay_period=15;

SHOW create stream replicated_table_for_alter1;
SHOW create stream replicated_table_for_alter2;

DROP STREAM IF EXISTS replicated_table_for_alter2;
DROP STREAM IF EXISTS replicated_table_for_alter1;

DROP STREAM IF EXISTS replicated_table_for_reset_setting1;
DROP STREAM IF EXISTS replicated_table_for_reset_setting2;

SET replication_alter_partitions_sync = 2;

create stream replicated_table_for_reset_setting1 (
 id uint64,
 Data string
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_00980_{database}/replicated_table_for_reset_setting', '1') ORDER BY id;

create stream replicated_table_for_reset_setting2 (
 id uint64,
 Data string
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_00980_{database}/replicated_table_for_reset_setting', '2') ORDER BY id;

SHOW create stream replicated_table_for_reset_setting1;
SHOW create stream replicated_table_for_reset_setting2;

ALTER STREAM replicated_table_for_reset_setting1 MODIFY SETTING index_granularity = 4096; -- { serverError 472 }

SHOW create stream replicated_table_for_reset_setting1;

ALTER STREAM replicated_table_for_reset_setting1 MODIFY SETTING merge_with_ttl_timeout = 100;
ALTER STREAM replicated_table_for_reset_setting2 MODIFY SETTING merge_with_ttl_timeout = 200;

SHOW create stream replicated_table_for_reset_setting1;
SHOW create stream replicated_table_for_reset_setting2;

DETACH TABLE replicated_table_for_reset_setting2;
ATTACH TABLE replicated_table_for_reset_setting2;

DETACH TABLE replicated_table_for_reset_setting1;
ATTACH TABLE replicated_table_for_reset_setting1;

SHOW create stream replicated_table_for_reset_setting1;
SHOW create stream replicated_table_for_reset_setting2;

-- ignore undefined setting
ALTER STREAM replicated_table_for_reset_setting1 RESET SETTING check_delay_period, unknown_setting;
ALTER STREAM replicated_table_for_reset_setting1 RESET SETTING merge_with_ttl_timeout;
ALTER STREAM replicated_table_for_reset_setting2 RESET SETTING merge_with_ttl_timeout;

SHOW create stream replicated_table_for_reset_setting1;
SHOW create stream replicated_table_for_reset_setting2;

DROP STREAM IF EXISTS replicated_table_for_reset_setting2;
DROP STREAM IF EXISTS replicated_table_for_reset_setting1;
