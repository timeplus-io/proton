DROP STREAM IF EXISTS t_99170;
CREATE STREAM t_99170 (x int32);

select sleep(1) FORMAT Null;

SELECT
    database = current_database() AS in_current_database,
    name
FROM system.tables
WHERE uuid = (
    SELECT uuid
    FROM system.tables
    WHERE database = current_database()
      AND name = 't_99170'
)
ORDER BY
    in_current_database,
    name;

SELECT metadata_modification_time
FROM system.tables
WHERE uuid = (
    SELECT uuid
    FROM system.tables
    WHERE database = current_database()
      AND name = 't_99170'
)
FORMAT Null;

DROP STREAM IF EXISTS t_99170;
