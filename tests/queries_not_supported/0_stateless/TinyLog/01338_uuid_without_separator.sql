SELECT to_uuid('417ddc5de5564d2795dda34d84e46a50');
SELECT to_uuid('417ddc5d-e556-4d27-95dd-a34d84e46a50');

DROP STREAM IF EXISTS t_uuid;
CREATE STREAM t_uuid (x uint8, y uuid, z string) ENGINE = TinyLog;

INSERT INTO t_uuid VALUES (1, '417ddc5de5564d2795dda34d84e46a50', 'Example 1');
INSERT INTO t_uuid VALUES (2, '417ddc5d-e556-4d27-95dd-a34d84e46a51', 'Example 2');

SELECT * FROM t_uuid ORDER BY x ASC;
DROP STREAM IF EXISTS t_uuid;
