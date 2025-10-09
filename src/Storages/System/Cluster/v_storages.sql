-- IMPORTANT: Change DDL COMMENT whenever query is changed. Provisioner only executes the SQL when COMMENT is changed.
-- If a MV which is not running in last 5 minutes, report error
CREATE OR REPLACE VIEW system.v_storages
AS
WITH storage_sizes AS
  (
    SELECT
      database, name, uuid, dimension AS store_type, any(state_string_value) AS stream_type, sum(state_value) AS total_bytes
    FROM
      system.stream_state_log
    WHERE
      (_tp_time > (now() - 5m)) AND (state_name = 'disk_size') AND (database != 'neutron')
    GROUP BY
      database, name, uuid, dimension
    UNION ALL
    SELECT
      database, name, uuid, 'ckpt_size' AS store_type, dimension AS stream_type, sum(state_value) AS total_bytes
    FROM
      system.stream_state_log
    WHERE
      (_tp_time > (now() - 5m)) AND state_name = 'checkpoint_storage_size'
    GROUP BY
      database, name, uuid, dimension
  )
SELECT
    database, name, uuid, store_type, stream_type, total_bytes
FROM
    storage_sizes
ORDER BY
    total_bytes DESC
SETTINGS query_mode = 'table'
COMMENT 'version 1';
