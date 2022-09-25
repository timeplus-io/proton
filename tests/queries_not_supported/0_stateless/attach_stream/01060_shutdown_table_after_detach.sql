-- Tags: no-parallel

DROP STREAM IF EXISTS test;
create stream test Engine = MergeTree ORDER BY number AS SELECT number, to_string(rand()) x from numbers(10000000);

SELECT count() FROM test;

ALTER STREAM test DETACH PARTITION tuple();

SELECT count() FROM test;

DETACH STREAM test;
ATTACH STREAM test;

ALTER STREAM test ATTACH PARTITION tuple();

SELECT count() FROM test;

DROP STREAM test;
