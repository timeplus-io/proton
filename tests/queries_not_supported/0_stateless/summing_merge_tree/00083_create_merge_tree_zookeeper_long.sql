-- Tags: long, zookeeper, no-replicated-database, no-parallel
-- Tag no-replicated-database: Old syntax is not allowed
-- Tag no-parallel: leftovers

SET optimize_on_insert = 0;

DROP STREAM IF EXISTS merge_tree;
DROP STREAM IF EXISTS collapsing_merge_tree;
DROP STREAM IF EXISTS versioned_collapsing_merge_tree;
DROP STREAM IF EXISTS summing_merge_tree;
DROP STREAM IF EXISTS summing_merge_tree_with_list_of_columns_to_sum;
DROP STREAM IF EXISTS aggregating_merge_tree;

DROP STREAM IF EXISTS merge_tree_with_sampling;
DROP STREAM IF EXISTS collapsing_merge_tree_with_sampling;
DROP STREAM IF EXISTS versioned_collapsing_merge_tree_with_sampling;
DROP STREAM IF EXISTS summing_merge_tree_with_sampling;
DROP STREAM IF EXISTS summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP STREAM IF EXISTS aggregating_merge_tree_with_sampling;

DROP STREAM IF EXISTS replicated_merge_tree;
DROP STREAM IF EXISTS replicated_collapsing_merge_tree;
DROP STREAM IF EXISTS replicated_versioned_collapsing_merge_tree;
DROP STREAM IF EXISTS replicated_summing_merge_tree;
DROP STREAM IF EXISTS replicated_summing_merge_tree_with_list_of_columns_to_sum;
DROP STREAM IF EXISTS replicated_aggregating_merge_tree;

DROP STREAM IF EXISTS replicated_merge_tree_with_sampling;
DROP STREAM IF EXISTS replicated_collapsing_merge_tree_with_sampling;
DROP STREAM IF EXISTS replicated_versioned_collapsing_merge_tree_with_sampling;
DROP STREAM IF EXISTS replicated_summing_merge_tree_with_sampling;
DROP STREAM IF EXISTS replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP STREAM IF EXISTS replicated_aggregating_merge_tree_with_sampling;


create stream merge_tree
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = MergeTree(d, (a, b), 111);
create stream collapsing_merge_tree
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = CollapsingMergeTree(d, (a, b), 111, y);
create stream versioned_collapsing_merge_tree
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = VersionedCollapsingMergeTree(d, (a, b), 111, y, b);
create stream summing_merge_tree
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = SummingMergeTree(d, (a, b), 111);
create stream summing_merge_tree_with_list_of_columns_to_sum
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = SummingMergeTree(d, (a, b), 111, (y, z));
create stream aggregating_merge_tree
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = AggregatingMergeTree(d, (a, b), 111);

create stream merge_tree_with_sampling
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = MergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
create stream collapsing_merge_tree_with_sampling
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = CollapsingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, y);
create stream versioned_collapsing_merge_tree_with_sampling
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = VersionedCollapsingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b, b), 111, y, b);
create stream summing_merge_tree_with_sampling
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = SummingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
create stream summing_merge_tree_with_sampling_with_list_of_columns_to_sum
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = SummingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, (y, z));
create stream aggregating_merge_tree_with_sampling
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = AggregatingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);

create stream replicated_merge_tree
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_merge_tree/', 'r1', d, (a, b), 111);
create stream replicated_collapsing_merge_tree
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/test_00083/01/replicated_collapsing_merge_tree/', 'r1', d, (a, b), 111, y);
create stream replicated_versioned_collapsing_merge_tree
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/test_00083/01/replicated_versioned_collapsing_merge_tree/', 'r1', d, (a, b), 111, y, b);
create stream replicated_summing_merge_tree
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test_00083/01/replicated_summing_merge_tree/', 'r1', d, (a, b), 111);
create stream replicated_summing_merge_tree_with_list_of_columns_to_sum
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test_00083/01/replicated_summing_merge_tree_with_list_of_columns_to_sum/', 'r1', d, (a, b), 111, (y, z));
create stream replicated_aggregating_merge_tree
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/test_00083/01/replicated_aggregating_merge_tree/', 'r1', d, (a, b), 111);

create stream replicated_merge_tree_with_sampling
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
create stream replicated_collapsing_merge_tree_with_sampling
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/test_00083/01/replicated_collapsing_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, y);
create stream replicated_versioned_collapsing_merge_tree_with_sampling
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/test_00083/01/replicated_versioned_collapsing_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b, b), 111, y, b);
create stream replicated_summing_merge_tree_with_sampling
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test_00083/01/replicated_summing_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
create stream replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test_00083/01/replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, (y, z));
create stream replicated_aggregating_merge_tree_with_sampling
	(d date, a string, b uint8, x string, y int8, z uint32) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/test_00083/01/replicated_aggregating_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);


INSERT INTO merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO versioned_collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO summing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO summing_merge_tree_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO aggregating_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

INSERT INTO merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO versioned_collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO summing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO summing_merge_tree_with_sampling_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO aggregating_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

INSERT INTO replicated_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_versioned_collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_summing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_summing_merge_tree_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_aggregating_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

INSERT INTO replicated_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_versioned_collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_summing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_aggregating_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);


DROP STREAM merge_tree;
DROP STREAM collapsing_merge_tree;
DROP STREAM versioned_collapsing_merge_tree;
DROP STREAM summing_merge_tree;
DROP STREAM summing_merge_tree_with_list_of_columns_to_sum;
DROP STREAM aggregating_merge_tree;

DROP STREAM merge_tree_with_sampling;
DROP STREAM collapsing_merge_tree_with_sampling;
DROP STREAM versioned_collapsing_merge_tree_with_sampling;
DROP STREAM summing_merge_tree_with_sampling;
DROP STREAM summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP STREAM aggregating_merge_tree_with_sampling;

DROP STREAM replicated_merge_tree;
DROP STREAM replicated_collapsing_merge_tree;
DROP STREAM replicated_versioned_collapsing_merge_tree;
DROP STREAM replicated_summing_merge_tree;
DROP STREAM replicated_summing_merge_tree_with_list_of_columns_to_sum;
DROP STREAM replicated_aggregating_merge_tree;

DROP STREAM replicated_merge_tree_with_sampling;
DROP STREAM replicated_collapsing_merge_tree_with_sampling;
DROP STREAM replicated_versioned_collapsing_merge_tree_with_sampling;
DROP STREAM replicated_summing_merge_tree_with_sampling;
DROP STREAM replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP STREAM replicated_aggregating_merge_tree_with_sampling;
