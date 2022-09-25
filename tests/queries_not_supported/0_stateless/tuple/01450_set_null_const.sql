DROP STREAM IF EXISTS test_mtree;

create stream test_mtree (`x` string, INDEX idx x TYPE set(10) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_mtree VALUES ('Hello, world');
SELECT count() FROM test_mtree WHERE x = NULL;

DROP STREAM test_mtree;
