-- IMPORTANT: Change DDL COMMENT whenever query is changed. Provisioner only executes the SQL when COMMENT is changed.
-- If a MV which is not running in last 5 minutes, report error
CREATE OR REPLACE VIEW system.v_failed_mat_views
AS
WITH running_mvs_in_last_5m AS
(
    SELECT
      database, name
    FROM system.introspection_state_log
    WHERE (_tp_time > (now() - 5m)) AND starts_with(dimension, 'materialized_view') AND (state_name = 'status') AND (state_string_value = 'Executing')
    ORDER BY _tp_time DESC -- order here to make sure we have the latest state
    SETTINGS query_mode = 'table'
)
SELECT database, name, state_string_value AS state, _tp_time
FROM system.introspection_state_log
WHERE (_tp_time > (now() - 5m)) AND starts_with(dimension, 'materialized_view') AND (state_name = 'status') AND NOT ((database, name) IN running_mvs_in_last_5m)
SETTINGS query_mode = 'table'
COMMENT 'version 3';
