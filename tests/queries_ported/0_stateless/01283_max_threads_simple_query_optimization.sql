DROP STREAM IF EXISTS data_01283;

set allow_asynchronous_read_from_io_pool_for_merge_tree = 0;
set remote_filesystem_read_method = 'read';
set local_filesystem_read_method = 'pread';
set load_marks_asynchronously = 0;

CREATE STREAM data_01283 engine=MergeTree()
ORDER BY key
PARTITION BY key
AS SELECT number key FROM numbers(10);

SET log_queries=1;
SELECT * FROM data_01283 LIMIT 1 FORMAT Null;
SET log_queries=0;
SYSTEM FLUSH LOGS;

-- 1 for PullingAsyncPipelineExecutor::pull
SELECT
    throw_if(count() != 1, 'no query was logged'),
    throw_if(length(thread_ids) != 2, 'too many threads used')
FROM system.query_log
WHERE current_database = current_database() AND type = 'QueryFinish' AND query LIKE '%data_01283 LIMIT 1%'
GROUP BY thread_ids
FORMAT Null;

DROP STREAM data_01283;
