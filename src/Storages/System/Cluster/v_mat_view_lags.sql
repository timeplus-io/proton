-- IMPORTANT: Change DDL COMMENT whenever query is changed. Provisioner only executes the SQL when COMMENT is changed.
CREATE OR REPLACE VIEW system.v_mat_view_lags
AS
WITH last_5m_progressing_status AS
(
  SELECT database, name, state_name, dimension, state_value, _tp_time AS ts
  FROM system.stream_state_log
  WHERE (_tp_time > (now() - 5m)) AND (state_name IN ('processed_sn', 'ckpt_sn', 'end_sn'))
  ORDER BY _tp_time DESC -- order here to make sure we have latest state
  SETTINGS query_mode = 'table'
),
latest_mv_lagging_per_source AS
(
  SELECT database, name, state_name, earliest(state_value) AS state_value, earliest(ts) AS ts
  FROM last_5m_progressing_status
  GROUP BY database, name, state_name, dimension
),
mv_lagging_aggr_per_state AS
( -- Aggregate all sources
  SELECT database, name, state_name, sum(state_value) AS state_value, earliest(ts) AS ts
  FROM last_5m_progressing_status
  GROUP BY database, name, state_name
),
mv_lagging_aggr_per_mv AS
(
  SELECT
      database,
      name,
      group_array(state_name) AS state_names,
      group_array(state_value) AS state_values,
      map_from_arrays(state_names, state_values) AS states,
      earliest(ts) AS ts
  FROM mv_lagging_aggr_per_state
  GROUP BY database, name
)
SELECT
    database,
    name,
    states['processed_sn'] AS processed_sn,
    states['ckpt_sn'] AS ckpt_sn,
    states['end_sn'] AS end_sn,
    if (end_sn != 0, end_sn - processed_sn, 0) AS lag,
    if (ckpt_sn != 0 AND processed_sn != 0, processed_sn - ckpt_sn, 0) AS ckpt_lag,
    ts
FROM mv_lagging_aggr_per_mv
COMMENT 'version 2';
