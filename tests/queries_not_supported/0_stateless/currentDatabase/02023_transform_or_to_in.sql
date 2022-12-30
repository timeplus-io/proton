DROP STREAM IF EXISTS t_transform_or;

create stream t_transform_or(B aggregate_function(uniq, string), A string) Engine=MergeTree ORDER BY (A);

INSERT INTO t_transform_or SELECT uniq_state(''), '0';

SELECT uniq_mergeIf(B, (A = '1') OR (A = '2') OR (A = '3'))
FROM cluster(test_cluster_two_shards, currentDatabase(), t_transform_or)
SETTINGS legacy_column_name_of_tuple_literal = 0;

SELECT uniq_mergeIf(B, (A = '1') OR (A = '2') OR (A = '3'))
FROM cluster(test_cluster_two_shards, currentDatabase(), t_transform_or)
SETTINGS legacy_column_name_of_tuple_literal = 1;

DROP STREAM t_transform_or;
