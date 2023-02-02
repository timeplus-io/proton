-- Tags: no-replicated-database, no-parallel, no-fasttest

CREATE STREAM test0 (
        c0 uint64
    ) ENGINE = MergeTree() PARTITION BY c0 ORDER BY c0;

SET allow_experimental_live_view=1;

CREATE LIVE VIEW live1 AS SELECT * FROM test0;

select 'ALTER LIVE VIEW live1 REFRESH';
ALTER LIVE VIEW live1 REFRESH; -- success

DROP STREAM test0;
DROP VIEW live1;
