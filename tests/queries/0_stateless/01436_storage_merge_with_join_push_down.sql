DROP STREAM IF EXISTS test1;
DROP STREAM IF EXISTS test1_distributed;
DROP STREAM IF EXISTS test_merge;

SET enable_optimize_predicate_expression = 1;

create stream test1 (id int64, name string) ENGINE MergeTree PARTITION BY (id) ORDER BY (id);
create stream test1_distributed AS test1 ENGINE = Distributed(test_cluster_two_shards_localhost, default, test1);
create stream test_merge AS test1 ENGINE = Merge('default', 'test1_distributed');

SELECT count() FROM test_merge
JOIN (SELECT 'anystring' AS name) AS n
USING name
WHERE id = 1;

DROP STREAM test1;
DROP STREAM test_merge;


create stream test1 (id int64, name string) ENGINE MergeTree PARTITION BY (id) ORDER BY (id);
create stream test_merge AS test1 ENGINE = Merge('default', 'test1');

SELECT count() FROM test_merge
JOIN (SELECT 'anystring' AS name) AS n
USING name
WHERE id = 1;

DROP STREAM test1;
DROP STREAM test_merge;
DROP STREAM test1_distributed;
