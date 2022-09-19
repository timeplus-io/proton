DROP STREAM IF EXISTS test1;
DROP STREAM IF EXISTS test2;
DROP STREAM IF EXISTS mat_view;

create stream test1 (a LowCardinality(string)) ENGINE=MergeTree() ORDER BY a;
create stream test2 (a uint64) engine=MergeTree() ORDER BY a;
CREATE MATERIALIZED VIEW test_mv TO test2 AS SELECT to_uint64(a = 'test') FROM test1;

DROP STREAM test_mv;
DROP STREAM test1;
DROP STREAM test2;
