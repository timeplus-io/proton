-- Tags: long, not_supported, blocked_by_SummingMergeTree

SELECT 'Replacing Merge Tree';
DROP STREAM IF EXISTS replacing_merge_tree;
create stream replacing_merge_tree (key uint32, date Datetime) ENGINE=ReplacingMergeTree() PARTITION BY date ORDER BY key;
INSERT INTO replacing_merge_tree VALUES (1, '2020-01-01'), (2, '2020-01-02'), (1, '2020-01-01'), (2, '2020-01-02');
SELECT * FROM replacing_merge_tree ORDER BY key;
DROP STREAM replacing_merge_tree;

SELECT 'Collapsing Merge Tree';
DROP STREAM IF EXISTS collapsing_merge_tree;
create stream collapsing_merge_tree (key uint32, sign int8, date Datetime) ENGINE=CollapsingMergeTree(sign) PARTITION BY date ORDER BY key;
INSERT INTO collapsing_merge_tree VALUES (1, 1, '2020-01-01'), (2, 1, '2020-01-02'), (1, -1, '2020-01-01'), (2, -1, '2020-01-02'), (1, 1, '2020-01-01');
SELECT * FROM collapsing_merge_tree ORDER BY key;
DROP STREAM collapsing_merge_tree;

SELECT 'Versioned Collapsing Merge Tree';
DROP STREAM IF EXISTS versioned_collapsing_merge_tree;
create stream versioned_collapsing_merge_tree (key uint32, sign int8, version int32, date Datetime) ENGINE=VersionedCollapsingMergeTree(sign, version) PARTITION BY date ORDER BY (key, version);
INSERT INTO versioned_collapsing_merge_tree VALUES (1, 1, 1, '2020-01-01'), (1, -1, 1, '2020-01-01'), (1, 1, 2, '2020-01-01');
SELECT * FROM versioned_collapsing_merge_tree ORDER BY key;
DROP STREAM versioned_collapsing_merge_tree;

SELECT 'Summing Merge Tree';
DROP STREAM IF EXISTS summing_merge_tree;
create stream summing_merge_tree (key uint32, val uint32, date Datetime) ENGINE=SummingMergeTree(val) PARTITION BY date ORDER BY key;
INSERT INTO summing_merge_tree VALUES (1, 1, '2020-01-01'), (2, 1, '2020-01-02'), (1, 5, '2020-01-01'), (2, 5, '2020-01-02');
SELECT * FROM summing_merge_tree ORDER BY key;
DROP STREAM summing_merge_tree;

SELECT 'Aggregating Merge Tree';
DROP STREAM IF EXISTS aggregating_merge_tree;
create stream aggregating_merge_tree (key uint32, val SimpleAggregateFunction(max, uint32), date Datetime) ENGINE=AggregatingMergeTree() PARTITION BY date ORDER BY key;
INSERT INTO aggregating_merge_tree VALUES (1, 1, '2020-01-01'), (2, 1, '2020-01-02'), (1, 5, '2020-01-01'), (2, 5, '2020-01-02');
SELECT * FROM aggregating_merge_tree ORDER BY key;
DROP STREAM aggregating_merge_tree;

SELECT 'Check creating empty parts';
DROP STREAM IF EXISTS empty;
create stream empty (key uint32, val uint32, date Datetime) ENGINE=SummingMergeTree(val) PARTITION BY date ORDER BY key;
INSERT INTO empty VALUES (1, 1, '2020-01-01'), (1, 1, '2020-01-01'), (1, -2, '2020-01-01');
SELECT * FROM empty ORDER BY key;
SELECT table, partition, active FROM system.parts where table = 'empty' and active = 1 and database = currentDatabase();
DROP STREAM empty;
