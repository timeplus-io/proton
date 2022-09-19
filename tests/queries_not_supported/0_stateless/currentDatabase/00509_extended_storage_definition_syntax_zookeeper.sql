-- Tags: zookeeper, no-parallel
-- Tag no-parallel: leftovers

SET optimize_on_insert = 0;

SELECT '*** Replicated with sampling ***';

DROP STREAM IF EXISTS replicated_with_sampling;

create stream replicated_with_sampling(x uint8)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00509/replicated_with_sampling', 'r1')
    ORDER BY x
    SAMPLE BY x;

INSERT INTO replicated_with_sampling VALUES (1), (128);
SELECT sum(x) FROM replicated_with_sampling SAMPLE 1/2;

DROP STREAM replicated_with_sampling;

SELECT '*** Replacing with implicit version ***';

DROP STREAM IF EXISTS replacing;

create stream replacing(d date, x uint32, s string) ENGINE = ReplacingMergeTree ORDER BY x PARTITION BY d;

INSERT INTO replacing VALUES ('2017-10-23', 1, 'a');
INSERT INTO replacing VALUES ('2017-10-23', 1, 'b');
INSERT INTO replacing VALUES ('2017-10-23', 1, 'c');

OPTIMIZE STREAM replacing PARTITION '2017-10-23' FINAL;

SELECT * FROM replacing;

DROP STREAM replacing;

SELECT '*** Replicated Collapsing ***';

DROP STREAM IF EXISTS replicated_collapsing;

create stream replicated_collapsing(d date, x uint32, sign int8)
    ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/test_00509/replicated_collapsing', 'r1', sign)
    PARTITION BY toYYYYMM(d) ORDER BY d;

INSERT INTO replicated_collapsing VALUES ('2017-10-23', 1, 1);
INSERT INTO replicated_collapsing VALUES ('2017-10-23', 1, -1), ('2017-10-23', 2, 1);

SYSTEM SYNC REPLICA replicated_collapsing;
OPTIMIZE STREAM replicated_collapsing PARTITION 201710 FINAL;

SELECT * FROM replicated_collapsing;

DROP STREAM replicated_collapsing;

SELECT '*** Replicated VersionedCollapsing ***';

DROP STREAM IF EXISTS replicated_versioned_collapsing;

create stream replicated_versioned_collapsing(d date, x uint32, sign int8, version uint8)
    ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/test_00509/replicated_versioned_collapsing', 'r1', sign, version)
    PARTITION BY toYYYYMM(d) ORDER BY (d, version);

INSERT INTO replicated_versioned_collapsing VALUES ('2017-10-23', 1, 1, 0);
INSERT INTO replicated_versioned_collapsing VALUES ('2017-10-23', 1, -1, 0), ('2017-10-23', 2, 1, 0);
INSERT INTO replicated_versioned_collapsing VALUES ('2017-10-23', 1, -1, 1), ('2017-10-23', 2, 1, 2);

SYSTEM SYNC REPLICA replicated_versioned_collapsing;
OPTIMIZE STREAM replicated_versioned_collapsing PARTITION 201710 FINAL;

SELECT * FROM replicated_versioned_collapsing;

DROP STREAM replicated_versioned_collapsing;

SELECT '*** Table definition with SETTINGS ***';

DROP STREAM IF EXISTS with_settings;

create stream with_settings(x uint32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00509/with_settings', 'r1')
    ORDER BY x
    SETTINGS replicated_can_become_leader = 0;

SELECT sleep(1); -- If replicated_can_become_leader were true, this replica would become the leader after 1 second.

SELECT is_leader FROM system.replicas WHERE database = currentDatabase() AND table = 'with_settings';

DROP STREAM with_settings;
