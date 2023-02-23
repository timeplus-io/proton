DROP STREAM IF EXISTS wikistat1;
DROP STREAM IF EXISTS wikistat2;

CREATE STREAM wikistat1
(
    time DateTime,
    project low_cardinality(string),
    subproject low_cardinality(string),
    path string,
    hits uint64,
    PROJECTION total
    (
        SELECT
            project,
            subproject,
            path,
            sum(hits),
            count()
        GROUP BY
            project,
            subproject,
            path
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/02494_zero_copy_and_projection', '1')
ORDER BY (path, time)
SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0, allow_remote_fs_zero_copy_replication=1, min_bytes_for_wide_part=0;

CREATE STREAM wikistat2
(
    time DateTime,
    project low_cardinality(string),
    subproject low_cardinality(string),
    path string,
    hits uint64,
    PROJECTION total
    (
        SELECT
            project,
            subproject,
            path,
            sum(hits),
            count()
        GROUP BY
            project,
            subproject,
            path
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/02494_zero_copy_and_projection', '2')
ORDER BY (path, time)
SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0, allow_remote_fs_zero_copy_replication=1, min_bytes_for_wide_part=0;

INSERT INTO wikistat1 SELECT to_datetime('2020-10-01 00:00:00'), 'hello', 'world', '/data/path', 10 from numbers(100);

INSERT INTO wikistat1 SELECT to_datetime('2020-10-01 00:00:00'), 'hello', 'world', '/data/path', 10 from numbers(99, 99);

SYSTEM SYNC REPLICA wikistat2;

SELECT count() from wikistat1 WHERE NOT ignore(*);
SELECT count() from wikistat2 WHERE NOT ignore(*);

SYSTEM STOP REPLICATION QUEUES wikistat2;

ALTER STREAM wikistat1 DELETE where time = to_datetime('2022-12-20 00:00:00') SETTINGS mutations_sync = 1;

SYSTEM START REPLICATION QUEUES wikistat2;

SYSTEM SYNC REPLICA wikistat2;

-- it doesn't make test flaky, rarely we will not delete the parts because of cleanup thread was slow.
-- Such condition will lead to successful queries.
SELECT 0 FROM numbers(5) WHERE sleepEachRow(1) = 1;

select sum(hits), count() from wikistat1 GROUP BY project, subproject, path settings allow_experimental_projection_optimization = 1, force_optimize_projection = 1;
select sum(hits), count() from wikistat2 GROUP BY project, subproject, path settings allow_experimental_projection_optimization = 1, force_optimize_projection = 1;

DROP STREAM wikistat1;
DROP STREAM wikistat2;
