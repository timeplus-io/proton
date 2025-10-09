DROP STREAM IF EXISTS sharded_table;
CREATE STREAM sharded_table (dummy uint8);
select sleep(1) FORMAT Null;

SET prefer_localhost_replica=0;
SELECT * FROM table(sharded_table) FORMAT Null SETTINGS log_comment='02985_shard_query_start_time_query_1';

SYSTEM FLUSH LOGS;

-- Check that there are 2 queries to shards and for each one query_start_time_microseconds is more recent
-- than initial_query_start_time_microseconds, and initial_query_start_time_microseconds matches the original query
-- query_start_time_microseconds
-- proton does not sent query to all shards and not support scalar subqueries,
WITH ref_queries AS (
  SELECT
    query_id, query_start_time, query_start_time_microseconds
  FROM
    system.query_log
  WHERE
    (event_date >= yesterday()) 
    AND (current_database = current_Database()) 
    AND (log_comment = '02985_shard_query_start_time_query_1') 
    AND (type = 'QueryFinish')
)
SELECT
  ql.type, 
  count_If(ql.query_start_time >= ql.initial_query_start_time) AS count_start_gte_initial_start, 
  count_If(ql.query_start_time_microseconds == ql.initial_query_start_time_microseconds) AS count_micros_gt_initial_micros, 
  count_If(ql.initial_query_start_time = rq.query_start_time) AS count_initial_start_eq_ref_start, 
  count_If(ql.initial_query_start_time_microseconds = rq.query_start_time_microseconds) AS count_initial_micros_eq_ref_micros
FROM
  system.query_log AS ql
INNER JOIN ref_queries AS rq ON ql.initial_query_id = rq.query_id
GROUP BY
  ql.type
