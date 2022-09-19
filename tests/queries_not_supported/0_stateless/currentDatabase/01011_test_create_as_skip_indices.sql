create stream foo (key int, INDEX i1 key TYPE minmax GRANULARITY 1) Engine=MergeTree() ORDER BY key;
create stream as_foo AS foo;
create stream dist (key int, INDEX i1 key TYPE minmax GRANULARITY 1) Engine=Distributed(test_shard_localhost, currentDatabase(), 'foo'); -- { serverError 36 }
create stream dist_as_foo Engine=Distributed(test_shard_localhost, currentDatabase(), 'foo') AS foo;

DROP STREAM foo;
DROP STREAM as_foo;
DROP STREAM dist_as_foo;
