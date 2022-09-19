-- Tags: zookeeper

SET check_query_single_value_result = 0;
SET send_logs_level = 'fatal';

DROP STREAM IF EXISTS mt_without_pk;

create stream mt_without_pk (SomeField1 int64, SomeField2 Double) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO mt_without_pk VALUES (1, 2);

CHECK TABLE mt_without_pk;

DROP STREAM IF EXISTS mt_without_pk;

DROP STREAM IF EXISTS replicated_mt_without_pk;

create stream replicated_mt_without_pk (SomeField1 int64, SomeField2 Double) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01037/replicated_mt_without_pk', '1') ORDER BY tuple();

INSERT INTO replicated_mt_without_pk VALUES (1, 2);

CHECK TABLE replicated_mt_without_pk;

DROP STREAM IF EXISTS replicated_mt_without_pk;
